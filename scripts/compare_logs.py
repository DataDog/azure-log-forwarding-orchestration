#!/usr/bin/env python
# stdlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from gzip import GzipFile
import os
from time import sleep, time

# 3p
import requests

# azure
from azure.storage.blob import BlobServiceClient

LOOKBACK_MINUTES = 120
END_MINUTES_AGO = 60

UUID_LENGTH = 36

CONTAINER = "insights-logs-functionapplogs"

STORAGE_SOURCE = "storage"
NATIVE_SOURCE = "native"
LFO_SOURCE = "lfo"
EVENT_HUB_SOURCE = "event_hub"

AZURE_LOGS_CONNECTION_STRING = os.environ.get("AzureWebJobsStorage")
DD_ARCHIVES_CONNECTION_STRING = os.environ.get("LOG_ARCHIVING")


def get_start_time(
    storage_id_map: dict[
        str, datetime
    ],  # storage_id_map is a dictionary of uuid to datetime
    storage_ids: list[str],  # storage_ids is a list of uuids
    lfo_ids: list[str],  # lfo_ids is a list of uuids
    native_ids: list[str],  # native_ids is a list of uuids
) -> datetime:
    """returns the first start time found in storage logs that is either lfo or native logs"""
    for storage_id in storage_ids:  # assumes storage_ids are sorted
        if storage_id in native_ids:
            submit_metric(
                "azure.log.forwarding.first_timestamp",
                1,
                tags={"source": NATIVE_SOURCE},
            )
            return storage_id_map[storage_id]
        if storage_id in lfo_ids:
            submit_metric(
                "azure.log.forwarding.first_timestamp",
                1,
                tags={"source": LFO_SOURCE},
            )
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
        curr_id = line[lorem_index - UUID_LENGTH + 1 : lorem_index - 1]
        if curr_id in blob_ids:
            duplicates += 1
        blob_ids[curr_id] = date_time_obj
    return blob_ids, duplicates


def submit_metric(metric_name: str, value: float | int, tags: dict | None = None):
    dd_site = os.getenv("DD_SITE", "datadoghq.com")
    api_key = os.getenv("DD_API_KEY")
    app_key = os.getenv("DD_APP_KEY")
    url = f"https://api.{dd_site}/api/v2/series"
    headers = {
        "Content-Type": "application/json",
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
    }
    metric = {
        "metric": metric_name,
        "type": 3,
        "points": [{"timestamp": int(time()), "value": value}],
    }
    if tags:
        tag_list = []
        for key in tags:
            tag_list.append(f"{key}:{tags[key]}")
        metric["tags"] = tag_list
    body = {
        "series": [
            metric,
        ]
    }
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()


def get_logs_from_storage_account(
    connection_string: str | None, filter, container: str = CONTAINER
) -> tuple[dict[str, datetime], int]:
    if not connection_string:
        raise Exception("No connection string found")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)
    blob_iter = container_client.list_blobs()
    hours_ago = datetime.now(timezone.utc) - timedelta(
        minutes=LOOKBACK_MINUTES + END_MINUTES_AGO
    )
    blob_list = list(blob_iter)
    log_ids = {}
    duplicates = 0
    for blob in blob_list:
        if not filter(blob):
            continue
        if blob.creation_time < hours_ago:
            continue
        blob_time_by_id, blob_duplicates = download_blob(
            blob_service_client, blob.name, container
        )
        duplicates += blob_duplicates
        # possible to have duplicates across blobs
        for blob_id in blob_time_by_id:
            if blob_id in log_ids:
                duplicates += 1
            log_ids[blob_id] = blob_time_by_id[blob_id]

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
    return truncated_data


def run():
    storage_timestamps_by_id = {}
    storage_ids = []
    native_ids = []
    lfo_ids = []

    end = datetime.now(timezone.utc) - timedelta(minutes=END_MINUTES_AGO)

    storage_timestamps_by_id, storage_duplicates = get_logs_from_storage_account(
        AZURE_LOGS_CONNECTION_STRING, lambda blob: "loggya" in blob.name.lower()
    )
    storage_ids = list(storage_timestamps_by_id.keys())
    storage_ids.sort(key=lambda e: storage_timestamps_by_id[e])
    if not storage_ids:
        raise Exception("No logs found in storage account")

    print(f"Duplicate storage ids: {storage_duplicates}")
    submit_metric(
        "azure.log.forwarding.duplicates",
        storage_duplicates,
        tags={"source": "storage"},
    )

    native_timestamps_by_id, native_duplicates = get_logs_from_storage_account(
        DD_ARCHIVES_CONNECTION_STRING, lambda blob: True, container="native"
    )
    native_ids = list(native_timestamps_by_id.keys())
    native_ids.sort(key=lambda e: native_timestamps_by_id[e])

    print(f"Duplicate liftr ids: {native_duplicates}")
    submit_metric(
        "azure.log.forwarding.duplicates",
        native_duplicates,
        tags={"source": NATIVE_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.totals",
        len(native_timestamps_by_id.keys()) + native_duplicates,
        tags={"source": NATIVE_SOURCE},
    )

    eventhub_timestamps_by_id, eventhub_duplicates = get_logs_from_storage_account(
        DD_ARCHIVES_CONNECTION_STRING, lambda blob: True, container="eventhub"
    )
    eventhub_ids = list(eventhub_timestamps_by_id.keys())
    eventhub_ids.sort(key=lambda e: eventhub_timestamps_by_id[e])

    print(f"Duplicate event hub ids: {eventhub_duplicates}")
    submit_metric(
        "azure.log.forwarding.duplicates",
        eventhub_duplicates,
        tags={"source": EVENT_HUB_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.totals",
        len(eventhub_timestamps_by_id.keys()) + eventhub_duplicates,
        tags={"source": EVENT_HUB_SOURCE},
    )

    lfo_timestamps_by_id, lfo_duplicates = get_logs_from_storage_account(
        DD_ARCHIVES_CONNECTION_STRING, lambda blob: True, container="lfo"
    )
    lfo_ids = list(lfo_timestamps_by_id.keys())
    lfo_ids.sort(key=lambda e: lfo_timestamps_by_id[e])

    print(f"Duplicate LFO ids: {lfo_duplicates}")
    submit_metric(
        "azure.log.forwarding.duplicates",
        lfo_duplicates,
        tags={"source": LFO_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.totals",
        len(lfo_timestamps_by_id.keys()) + lfo_duplicates,
        tags={"source": LFO_SOURCE},
    )

    start = get_start_time(storage_timestamps_by_id, storage_ids, lfo_ids, native_ids)
    print(f"Start time: {start}, end time: {end}")
    native_truncated = truncate_to_set(native_ids, start, end, native_timestamps_by_id)
    lfo_truncated = truncate_to_set(lfo_ids, start, end, lfo_timestamps_by_id)
    eventhub_truncated = truncate_to_set(
        eventhub_ids, start, end, eventhub_timestamps_by_id
    )
    storage_truncated = truncate_to_set(
        storage_ids, start, end, storage_timestamps_by_id
    )

    print(
        f"Storage ids: {len(storage_truncated)}, LFO ids: {len(lfo_truncated)}, Native ids: {len(native_truncated)}, Event Hub ids: {len(eventhub_truncated)}"
    )

    missing_lfo_ids = storage_truncated - lfo_truncated
    print(f"Missing LFO ids: {len(missing_lfo_ids)}")
    submit_metric(
        "azure.log.forwarding.missing_percent",
        (len(missing_lfo_ids) * 1.0 / len(storage_truncated)),
        tags={"source": LFO_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.missing",
        len(missing_lfo_ids),
        tags={"source": LFO_SOURCE},
    )

    missing_native_ids = storage_truncated - native_truncated
    print(f"Missing native ids: {len(missing_native_ids)}")

    submit_metric(
        "azure.log.forwarding.missing_percent",
        (len(missing_native_ids) * 1.0 / len(storage_truncated)),
        tags={"source": NATIVE_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.missing",
        len(missing_native_ids),
        tags={"source": NATIVE_SOURCE},
    )

    missing_eventhub_ids = storage_truncated - eventhub_truncated
    print(f"Missing event hub ids: {len(missing_eventhub_ids)}")
    submit_metric(
        "azure.log.forwarding.missing_percent",
        (len(missing_eventhub_ids) * 1.0 / len(storage_truncated)),
        tags={"source": EVENT_HUB_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.missing",
        len(missing_eventhub_ids),
        tags={"source": EVENT_HUB_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.found",
        len(storage_truncated),
        tags={"source": STORAGE_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.found",
        len(lfo_truncated),
        tags={"source": LFO_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.found",
        len(native_truncated),
        tags={"source": NATIVE_SOURCE},
    )

    submit_metric(
        "azure.log.forwarding.found",
        len(eventhub_truncated),
        tags={"source": EVENT_HUB_SOURCE},
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
