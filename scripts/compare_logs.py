#!/usr/bin/env python
# stdlib
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from io import BytesIO
from gzip import GzipFile
import os
from time import time

# 3p
import requests

# azure
from azure.storage.blob import BlobServiceClient

LOOKBACK_MINUTES = 60
END_MINUTES_AGO = 15

UUID_LENGTH = 36

CONTAINER = "insights-logs-functionapplogs"

STORAGE_SOURCE = "storage"
NATIVE_SOURCE = "native"
LFO_SOURCE = "lfo"
EVENT_HUB_SOURCE = "event_hub"

DD_ARCHIVES_CONNECTION_STRING = os.environ.get("LOG_ARCHIVING")

LogID = namedtuple("LogID", ["uuid", "dd_id", "timestamp"])


def get_start_time(
    storage_ids: list[LogID],  # storage_ids is a list of uuids
    lfo_ids: list[LogID],  # lfo_ids is a list of uuids
    native_ids: list[LogID],  # native_ids is a list of uuids
) -> datetime:
    """returns the first start time found in storage logs that is either lfo or native logs"""
    for storage_id in storage_ids:  # assumes storage_ids are sorted
        for lfo_id in lfo_ids:
            if lfo_id.uuid == storage_id.uuid:
                submit_metric(
                    "azure.log.forwarding.first_timestamp",
                    1,
                    tags={"source": LFO_SOURCE},
                )
                return storage_id.timestamp
        for native_id in native_ids:
            if native_id.uuid == storage_id.uuid:
                submit_metric(
                    "azure.log.forwarding.first_timestamp",
                    1,
                    tags={"source": NATIVE_SOURCE},
                )
                return storage_id.timestamp
    raise Exception("No common start time found")


def download_blob(
    blob_service_client: BlobServiceClient, blob_name: str, container_name: str
) -> list[LogID]:
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    # encoding param is necessary for readall() to return str, otherwise it returns bytes
    downloader = blob_client.download_blob(max_concurrency=1, encoding="UTF-8")
    blob_ids = list()
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
            return blob_ids
        print(f"Error downloading blob {blob_name}: {e}")
        raise e
    # print(f"Downloaded {len(content)} bytes from {blob_name}")
    print(".", end="", flush=True)
    for line in content.split("\n"):
        if line and len(line) < 31:
            continue
        if (
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam euismod, nisl eget aliquam ultricies, nunc nisl aliquet nunc, sed aliquam"
            not in line
        ):
            continue
        dd_id = None
        if blob_name[len(blob_name) - 2 :] == "gz":
            date_time_obj = datetime.strptime(line[9:33], "%Y-%m-%dT%H:%M:%S.%fZ")
            id_index = line.find(',"_id":"')
            if id_index != -1:
                dd_id = line[id_index + 8 : line.find('","', id_index + 8)]
        else:
            date_time_obj = datetime.strptime(line[11:31], "%Y-%m-%dT%H:%M:%SZ")
        date_time_obj = date_time_obj.replace(tzinfo=timezone.utc)
        lorem_index = line.find("Lorem")
        curr_id = line[lorem_index - UUID_LENGTH + 1 : lorem_index - 1]
        log_id = LogID(uuid=curr_id, dd_id=dd_id, timestamp=date_time_obj)
        blob_ids.append(log_id)

    return blob_ids


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
) -> list[LogID]:
    if not connection_string:
        raise Exception("No connection string found")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container)
    blob_iter = container_client.list_blobs()
    hours_ago = datetime.now(timezone.utc) - timedelta(
        minutes=LOOKBACK_MINUTES + END_MINUTES_AGO
    )
    blob_list = list(blob_iter)
    log_ids = list()
    blob_futures = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for blob in blob_list:
            if not filter(blob):
                continue
            if blob.creation_time < hours_ago:
                continue
            blob_futures.append(
                executor.submit(
                    download_blob, blob_service_client, blob.name, container
                )
            )

    for future in blob_futures:
        blob_ids = future.result()
        log_ids.extend(blob_ids)
    log_ids.sort(key=lambda e: e.timestamp)
    return log_ids


def get_duplicates(input: list[LogID]) -> list[LogID]:
    log_ids = list()
    seen = set()
    for log_id in input:
        if log_id.uuid in seen:
            log_ids.append(log_id)
        else:
            seen.add(log_id.uuid)
    return log_ids


def truncate_to_set(
    data: list[LogID], start: datetime, end: datetime, callable=lambda x: x
) -> set[str]:
    truncated_data = set()
    for item in data:
        if item.timestamp > start and item.timestamp < end:
            truncated_data.add(callable(item))
    return truncated_data


def run():
    with ThreadPoolExecutor() as executor:
        node_20_ids_future = executor.submit(
            get_logs_from_storage_account,
            DD_ARCHIVES_CONNECTION_STRING,
            lambda _: True,
            container="node20",
        )
        node_18_ids_future = executor.submit(
            get_logs_from_storage_account,
            DD_ARCHIVES_CONNECTION_STRING,
            lambda _: True,
            container="node18",
        )

    node_20_ids = node_20_ids_future.result()
    node_18_ids = node_18_ids_future.result()

    node_20_duplicates = {log.dd_id for log in get_duplicates(node_20_ids)}
    node_18_duplicates = {log.dd_id for log in get_duplicates(node_18_ids)}
    print()
    print(
        f"Duplicate node 20 ids: {len(node_20_duplicates)}, Unique: {len(node_20_ids)-len(node_20_duplicates)}"
    )
    print(
        f"Duplicate node 18 ids: {len(node_18_duplicates)}, Unique: {len(node_18_ids)-len(node_18_duplicates)}"
    )
    # calculate percentage of duplicated logs of each
    if node_20_ids:
        percentage = len(node_20_duplicates) * 100.0 / len(node_20_ids)
        print(f"Percentage of duplicated node 20 logs: {percentage:.2f}%")
    if node_18_ids:
        percentage = len(node_18_duplicates) * 100.0 / len(node_18_ids)
        print(f"Percentage of duplicated node 18 logs: {percentage:.2f}%")


if __name__ == "__main__":
    run()
