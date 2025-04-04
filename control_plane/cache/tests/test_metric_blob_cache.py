# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from datetime import datetime, timedelta
from json import dumps
from unittest import TestCase

from cache.metric_blob_cache import deserialize_blob_metric_entry
from tasks.scaling_task import METRIC_COLLECTION_PERIOD_MINUTES


class TestMetricBlobCache(TestCase):
    def setUp(self) -> None:
        self.current_time = (datetime.now()).timestamp()
        self.oldest_legal_time = datetime.now() - timedelta(minutes=METRIC_COLLECTION_PERIOD_MINUTES)
        self.old_time = (self.oldest_legal_time - timedelta(minutes=5)).timestamp()
        return super().setUp()

    def test_normal_validation(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.current_time,
                "runtime_seconds": 2.11,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
                "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
                "version": "74a5f6a",
            }
        )
        expected_dict = {
            "timestamp": self.current_time,
            "runtime_seconds": 2.11,
            "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
            "version": "74a5f6a",
        }
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNotNone(returned_dict)
        if returned_dict:
            self.assertDictEqual(returned_dict, expected_dict)

    def test_validation_old_time(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.old_time,
                "runtime_seconds": 2.11,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
                "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
                "version": "74a5f6a",
            }
        )
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNone(returned_dict)

    def test_validation_oldest_legal_time(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.oldest_legal_time.timestamp(),
                "runtime_seconds": 2.11,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
                "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
                "version": "74a5f6a",
            }
        )
        expected_dict = {
            "timestamp": self.oldest_legal_time.timestamp(),
            "runtime_seconds": 2.11,
            "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
            "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
            "version": "74a5f6a",
        }
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNotNone(returned_dict)
        if returned_dict:
            self.assertDictEqual(returned_dict, expected_dict)

    def test_validation_missing_property(self):
        blob_dict_str = dumps(
            {
                "runtime_seconds": 2.11,
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
                "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
            }
        )
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNone(returned_dict)

    def test_validation_too_many_properties(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.old_time,
                "runtime_seconds": 2.11,
                "hello": "my_friend",
                "resource_log_volume": {"5a095f74c60a": 4, "93a5885365f5": 6},
                "resource_log_bytes": {"5a095f74c60a": 40, "93a5885365f5": 60},
                "version": "74a5f6a",
            }
        )
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNone(returned_dict)
