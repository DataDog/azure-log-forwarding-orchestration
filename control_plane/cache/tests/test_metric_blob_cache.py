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
                "runtimeSeconds": 2.11,
                "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
            }
        )
        expected_dict = {
            "timestamp": self.current_time,
            "runtimeSeconds": 2.11,
            "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
        }
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNotNone(returned_dict)
        if returned_dict:
            self.assertDictEqual(returned_dict, expected_dict)

    def test_validation_old_time(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.old_time,
                "runtimeSeconds": 2.11,
                "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
            }
        )
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNone(returned_dict)

    def test_validation_oldest_legal_time(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.oldest_legal_time.timestamp(),
                "runtimeSeconds": 2.11,
                "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
            }
        )
        expected_dict = {
            "timestamp": self.oldest_legal_time.timestamp(),
            "runtimeSeconds": 2.11,
            "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
        }
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNotNone(returned_dict)
        if returned_dict:
            self.assertDictEqual(returned_dict, expected_dict)

    def test_validation_missing_property(self):
        blob_dict_str = dumps(
            {
                "runtimeSeconds": 2.11,
                "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
            }
        )
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNone(returned_dict)

    def test_validation_too_many_properties(self):
        blob_dict_str = dumps(
            {
                "timestamp": self.old_time,
                "runtimeSeconds": 2.11,
                "hello": "my_friend",
                "resourceLogVolumes": {"5a095f74c60a": 4, "93a5885365f5": 6},
            }
        )
        returned_dict = deserialize_blob_metric_entry(blob_dict_str, self.oldest_legal_time.timestamp())
        self.assertIsNone(returned_dict)
