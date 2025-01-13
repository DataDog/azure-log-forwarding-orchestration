#!/usr/bin/env python
# stdlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from gzip import GzipFile
import json
import os
from time import sleep, time

# 3p
import requests

# azure
from azure.storage.blob import BlobServiceClient

LOOKBACK_MINUTES = 120
CONTAINER = "insights-logs-functionapplogs"

START_TIME = datetime.now()

# storage_id_map = {}
# storage_ids = []
# native_ids = []
# lfo_ids = []
# event_hub_ids = []


def get_dd_time(time: datetime) -> str:
    # "2020-10-07T00:00:00+00:00"
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def get_uuid_from_log(log) -> str | None:
    message = json.dumps(log)

    if (
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam euismod, nisl eget aliquam ultricies, nunc nisl aliquet nunc, sed aliquam"
        not in message
    ):
        return None
    lorem_index = message.find("Lorem")
    return message[lorem_index - 37 : lorem_index - 1]


def get_uuids(data) -> list:
    curr_ids = []
    ids_to_timestamps = {}

    def uuid_sort(e):
        return ids_to_timestamps[e]

    for item in data:
        curr_uuid = get_uuid_from_log(item)
        if not curr_uuid:
            continue
        curr_ids.append(curr_uuid)
        date_time_obj = None
        try:
            date_time_obj = datetime.strptime(
                item.get("attributes", {}).get("timestamp"), "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        except Exception:
            date_time_obj = datetime.strptime(
                item.get("attributes", {}).get("timestamp"), "%Y-%m-%dT%H:%M:%SZ"
            )
        ids_to_timestamps[curr_uuid] = date_time_obj
    curr_ids.sort(key=uuid_sort)
    return curr_ids


def get_start_time(
    storage_id_map: dict[str, datetime],
    storage_ids: list[str],
    lfo_ids: list[str],
    native_ids: list[str],
) -> datetime:
    for storage_id in storage_ids:
        if storage_id in native_ids or storage_id in lfo_ids:
            return storage_id_map[storage_id]
    raise Exception("No common start time found")


def download_blob(
    blob_service_client: BlobServiceClient, blob_name: str, container_name: str
) -> tuple[dict[str, datetime], int]:
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    duplicates = 0
    # encoding param is necessary for readall() to return str, otherwise it returns bytes
    downloader = blob_client.download_blob(max_concurrency=1, encoding="UTF-8")
    blob_ids = {}
    try:
        if blob_name[len(blob_name) - 2 :] == "gz":
            stream = BytesIO()
            downloader.readinto(stream)
            if not stream.getvalue():
                content = ""
            else:
                stream.seek(0)
                decompressed_file = GzipFile(fileobj=stream, mode="rb")
                content = decompressed_file.read().decode("utf-8")
        else:
            content = downloader.readall()
    except Exception as e:
        if "The condition specified using HTTP conditional" in str(e):
            print(f"Blob {blob_name} was being WEIRD")
            return blob_ids, duplicates
        print(f"Error downloading blob {blob_name}: {e}")
        raise e
    print(f"Downloaded {len(content)} bytes from {blob_name}")
    for line in content.split("\n"):
        if line and len(line) < 31:
            continue
        if (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam euismod, nisl eget aliquam ultricies, nunc nisl aliquet nunc, sed aliquam"
            not in line
        ):
            continue
        if blob_name[len(blob_name) - 2 :] == "gz":
            date_time_obj = datetime.strptime(line[9:33], "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            date_time_obj = datetime.strptime(line[11:31], "%Y-%m-%dT%H:%M:%SZ")
        date_time_obj = date_time_obj.replace(tzinfo=timezone.utc)
        lorem_index = line.find("Lorem")
        curr_id = line[lorem_index - 37 : lorem_index - 1]
        if curr_id in blob_ids:
            duplicates += 1
        blob_ids[curr_id] = date_time_obj
    return blob_ids, duplicates


def submit_metric(metric_name: str, value: float | int):
    # # Template variables
    # export NOW="$(date +%s)"
    # # Curl command
    # curl -X POST "https://api.datadoghq.com/api/v2/series" \
    # -H "Accept: application/json" \
    # -H "Content-Type: application/json" \
    # -H "DD-API-KEY: ${DD_API_KEY}" \
    # -d @- << EOF
    # {
    #   "series": [
    #     {
    #       "metric": "system.load.1",
    #       "type": 0,
    #       "points": [
    #         {
    #           "timestamp": 1636629071,
    #           "value": 0.7
    #         }
    #       ],
    #       "resources": [
    #         {
    #           "name": "dummyhost",
    #           "type": "host"
    #         }
    #       ]
    #     }
    #   ]
    # }
    # EOF
    dd_site = os.getenv("DD_SITE", "datadoghq.com")
    api_key = os.getenv("DD_API_KEY")
    app_key = os.getenv("DD_APP_KEY")
    url = f"https://api.{dd_site}/api/v2/series"
    headers = {
        "Content-Type": "application/json",
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
    }
    body = {
        "series": [
            {
                "metric": metric_name,
                "type": 3,
                "points": [{"timestamp": int(time()), "value": value}],
            }
        ]
    }
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()


def get_logs(query="*", from_time="now-4h", to_time="now"):
    # curl -L -X POST "https://api.us3.datadoghq.com/api/v2/logs/events/search" -H "Content-Type: application/json" -H "DD-API-KEY: <DATADOG_API_KEY>" -H "DD-APPLICATION-KEY: <DATADOG_APP_KEY>" --data-raw '{
    #   "filter": {
    #     "from": "2020-10-07T00:00:00+00:00",
    #     "to": "2020-10-07T00:15:00+00:00",
    #     "query": "*"
    #   },
    #    "page": {
    #      "cursor": "eyJhZnRlciI6IkFRQUFBWFVBWFZOU3Z1TXZXd0FBQUFCQldGVkJXRlpPVTJJMlpXY3hYM2MyTFZWQlFRIiwidmFsdWVzIjpbIjUwMCJdfQ",
    #     "limit":2
    #   },
    #   "sort":"-@pageViews"
    # }'
    results = []
    dd_site = os.getenv("DD_SITE", "datadoghq.com")
    api_key = os.getenv("DD_API_KEY")
    app_key = os.getenv("DD_APP_KEY")
    url = f"https://api.{dd_site}/api/v2/logs/events/search"
    headers = {
        "Content-Type": "application/json",
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
    }
    body = {
        "filter": {"from": from_time, "to": to_time, "query": query},
        "page": {"limit": 1000},
    }

    while True:
        response = requests.post(url, headers=headers, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            if response.status_code == 429:
                sleep_time = int(response.headers.get("x-ratelimit-reset", 10)) + 1
                print(f"Rate limit exceeded, waiting {sleep_time} seconds.")
                sleep(sleep_time)
                continue
        response_json = response.json()
        data = response_json.get("data", [])
        print(f"Received {len(data)} logs for query {query}")
        results.extend(data)

        if not response_json.get("meta", {}).get("page", {}).get("after"):
            # if True:
            break
        body["page"]["cursor"] = response_json["meta"]["page"]["after"]
    return results


def store_logs(logs: list, source: str):
    # save logs to file named after source
    file_name = f"{source}_{START_TIME.year}{START_TIME.month}{START_TIME.day}{START_TIME.hour}{START_TIME.minute}.json"
    print(f"Saving {len(logs)} logs to {file_name}")
    with open(file_name, "w") as f:
        json.dump(logs, f)


def get_logs_from_storage_account(
    connection_string: str | None, filter, container: str = CONTAINER
) -> tuple[dict[str, datetime], int]:
    if not connection_string:
        raise Exception("No connection string found")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)
    blob_iter = container_client.list_blobs()
    hours_ago = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES + 60)
    blob_list = list(blob_iter)
    log_ids = {}
    duplicates = 0
    for blob in blob_list:
        # blob_client = container_client.get_blob_client(blob.name)
        # if "loggya" not in blob.name.lower():
        if not filter(blob):
            continue
        if blob.creation_time < hours_ago:
            continue
        blob_ids, blob_duplicates = download_blob(
            blob_service_client, blob.name, container
        )
        duplicates += blob_duplicates
        # possible to have duplicates across blobs
        for blob_id in blob_ids:
            if blob_id in log_ids:
                duplicates += 1
            log_ids[blob_id] = blob_ids[blob_id]

    # def id_sort(e):
    #     return storage_id_map[e]

    # storage_ids.sort(key=id_sort)
    return log_ids, duplicates


def truncate_to_set(
    data: list, start: datetime, end: datetime, storage_id_map: dict[str, datetime]
) -> set:
    truncated_data = set()
    for item in data:
        if (
            storage_id_map.get(item)
            and storage_id_map[item] > start
            and storage_id_map[item] < end
        ):
            truncated_data.add(item)
        # elif not storage_id_map.get(item):
        #     print(f"Could not find timestamp for {item}")
    return truncated_data


lfo_query = "@resource_name:loggya control_plane_id:f2f2da2c9b64"
native_query = '"loggya" forwarder:native'
event_hub_query = '"loggya" forwarder:eventhub'

connection_string = os.environ.get("AzureWebJobsStorage")


archives_connection_string = os.environ.get("LOG_ARCHIVING")


def run():
    storage_id_map = {}
    storage_ids = []
    native_ids = []
    lfo_ids = []

    end = datetime.now(timezone.utc) - timedelta(minutes=60)
    # endtime = get_dd_time(end)
    # starttime = get_dd_time(end - timedelta(minutes=LOOKBACK_MINUTES))
    storage_id_map, storage_duplicates = get_logs_from_storage_account(
        connection_string, lambda blob: "loggya" in blob.name.lower()
    )
    storage_ids = list(storage_id_map.keys())
    storage_ids.sort(key=lambda e: storage_id_map[e])
    if not storage_ids:
        raise Exception("No logs found in storage account")

    print(f"Duplicate storage ids: {storage_duplicates}")
    submit_metric(
        "azure.log.forwarding.storage.duplicate",
        storage_duplicates,
    )

    # native_logs = get_logs(query=native_query, from_time=starttime, to_time=endtime)
    # store_logs(native_logs, "native")
    # native_ids = get_uuids(native_logs)
    native_id_map, native_duplicates = get_logs_from_storage_account(
        archives_connection_string, lambda blob: True, container="native"
    )
    native_ids = list(native_id_map.keys())
    native_ids.sort(key=lambda e: native_id_map[e])

    print(f"Duplicate liftr ids: {native_duplicates}")
    submit_metric(
        "azure.log.forwarding.native.duplicate",
        native_duplicates,
    )

    # event_hub_logs = get_logs(query=event_hub_query, from_time=starttime, to_time=endtime)
    # store_logs(event_hub_logs, "event_hub")
    # event_hub_ids = get_uuids(event_hub_logs)

    # lfo_logs = get_logs(query=lfo_query, from_time=starttime, to_time=endtime)
    # store_logs(lfo_logs, "lfo")
    # lfo_ids = get_uuids(lfo_logs)
    lfo_id_map, lfo_duplicates = get_logs_from_storage_account(
        archives_connection_string, lambda blob: True, container="lfo"
    )
    lfo_ids = list(lfo_id_map.keys())
    lfo_ids.sort(key=lambda e: lfo_id_map[e])

    print(f"Duplicate LFO ids: {lfo_duplicates}")
    submit_metric(
        "azure.log.forwarding.lfo.duplicate",
        lfo_duplicates,
    )

    start = get_start_time(storage_id_map, storage_ids, lfo_ids, native_ids)
    print(f"Start time: {start}, end time: {end}")
    native_truncated = truncate_to_set(native_ids, start, end, native_id_map)
    lfo_truncated = truncate_to_set(lfo_ids, start, end, lfo_id_map)
    # event_hub_truncated = truncate_list(event_hub_ids, start, end, event_hub_ids)
    storage_truncated = truncate_to_set(storage_ids, start, end, storage_id_map)

    print(
        f"Storage ids: {len(storage_truncated)}, LFO ids: {len(lfo_truncated)}, Native ids: {len(native_truncated)}"
    )

    missing_lfo_ids = storage_truncated - lfo_truncated
    print(f"Missing LFO ids: {len(missing_lfo_ids)}")
    submit_metric(
        "azure.log.forwarding.lfo.missing",
        (len(missing_lfo_ids) * 1.0 / len(storage_truncated)),
    )

    missing_native_ids = storage_truncated - native_truncated
    print(f"Missing native ids: {len(missing_native_ids)}")

    submit_metric(
        "azure.log.forwarding.native.missing",
        (len(missing_native_ids) * 1.0 / len(storage_truncated)),
    )


while True:
    print(f"Starting at {datetime.now()}")
    run()
    try:
        run()
    except Exception as e:
        print(f"Error: {e}")
    print(f"Done at {datetime.now()}")
    sleep(60)
