from unittest import TestCase
from json import dumps
from cache.diagnostic_settings_cache import (
    DiagnosticSettingsCache,
    deserialize_diagnostic_settings_cache,
)

from cache.tests import sub_id1, sub_id2


class TestDeserializeDiagnosticSettingsCache(TestCase):
    def test_valid_cache(self):
        diagnostic_settings_cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1": {"id": "hi", "type": "eventhub", "event_hub_name": "eh", "event_hub_namespace": "ehn"},
                "resource2": {
                    "id": "1234",
                    "type": "storageaccount",
                    "storage_account_id": "some_resource_id",
                },
            },
            sub_id2: {
                "resource3": {
                    "id": "5678",
                    "type": "eventhub",
                    "event_hub_name": "eh3",
                    "event_hub_namespace": "ehn",
                }
            },
        }
        cache_str = dumps(diagnostic_settings_cache)
        success, cache = deserialize_diagnostic_settings_cache(cache_str)
        self.assertTrue(success)
        self.assertEqual(
            cache,
            diagnostic_settings_cache,
        )

    def test_invalid_json_is_invalid(self):
        cache_str = "{invalid_json}"
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_not_dict_cache_is_invalid(self):
        cache_str = dumps(["not_a_dict"])
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_non_dict_resources_config_is_invalid(self):
        cache_str = dumps({sub_id1: "not_a_dict"})
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_some_non_dict_resource_configs(self):
        cache_str = dumps(
            {
                sub_id1: {
                    "resource1": {
                        "id": "1234",
                        "type": "storageaccount",
                        "storage_account_id": "some_resource_id",
                    }
                },
                sub_id2: {"resource2": ["not_a_dict"]},
            }
        )
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_non_dict_configs(self):
        cache_str = dumps(
            {sub_id1: {"resource1": "setting1", "resource2": "setting2"}, sub_id2: {"resource3": "setting3"}}
        )
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_missing_config_keys(self):
        cache_str = dumps({sub_id1: {"region2": {"resource1": {}}}})
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_partial_missing_config_keys(self):
        cache_str = dumps({sub_id1: {"resource1": {"id": "hi", "event_hub_name": "eh"}}})
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_mismatch_config_forwarding_type(self):
        cache_str = dumps(
            {sub_id1: {"resource1": {"id": "hi", "type": "eventhub", "storage_account_id": "storage123"}}}
        )
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)
