from unittest import TestCase
from json import dumps
from unittest.mock import patch
from diagnostic_settings_task.cache import (
    DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG,
    DiagnosticSettingsCache,
    deserialize_diagnostic_settings_cache,
    deserialize_resource_cache,
    ResourceCacheError,
)

sub_id1 = "f7a0a345-103e-4b6e-93b7-13d0a56fd363"
sub_id2 = "422f4b27-5493-4450-b308-0118e123ca89"


class TestDeserializeResourceCache(TestCase):
    def test_valid_cache(self):
        cache_str = dumps({sub_id1: ["resource1", "resource2"], sub_id2: ["resource3"]})
        self.assertEqual(
            deserialize_resource_cache(cache_str),
            {sub_id1: frozenset(["resource1", "resource2"]), sub_id2: frozenset(["resource3"])},
        )

    def test_invalid_json(self):
        cache_str = "{invalid_json}"
        with self.assertRaises(ResourceCacheError):
            deserialize_resource_cache(cache_str)

    def test_not_dict(self):
        cache_str = dumps(["not_a_dict"])
        with self.assertRaises(ResourceCacheError):
            deserialize_resource_cache(cache_str)

    def test_dict_with_non_list_values(self):
        cache_str = dumps({sub_id1: "not_a_list"})
        with self.assertRaises(ResourceCacheError):
            deserialize_resource_cache(cache_str)

    def test_dict_with_some_non_list_values(self):
        cache_str = dumps({sub_id1: ["r1"], sub_id2: {"hi": "value"}})
        with self.assertRaises(ResourceCacheError):
            deserialize_resource_cache(cache_str)


class TestDeserializeDiagnosticSettingsCache(TestCase):
    def setUp(self) -> None:
        log_patch = patch("diagnostic_settings_task.cache.log")
        self.addCleanup(log_patch.stop)
        self.log = log_patch.start()

    def test_valid_cache(self):
        diagnostic_settings_cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1": {"diagnostic_setting_id": "hi", "event_hub_name": "eh", "event_hub_namespace": "ehn"},
                "resource2": {"diagnostic_setting_id": "1234", "event_hub_name": "eh2", "event_hub_namespace": "ehn2"},
            },
            sub_id2: {
                "resource3": {"diagnostic_setting_id": "5678", "event_hub_name": "eh3", "event_hub_namespace": "ehn"}
            },
        }
        cache_str = dumps(diagnostic_settings_cache)
        self.assertEqual(
            deserialize_diagnostic_settings_cache(cache_str),
            diagnostic_settings_cache,
        )

    def test_invalid_json(self):
        cache_str = "{invalid_json}"
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)

    def test_not_dict(self):
        cache_str = dumps(["not_a_dict"])
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)

    def test_non_dict_resource_configs(self):
        cache_str = dumps({sub_id1: "not_a_dict"})
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)

    def test_some_non_dict_resource_configs(self):
        cache_str = dumps({sub_id1: {"r1": "setting1"}, sub_id2: ["not_a_dict"]})
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)

    def test_non_dict_configs(self):
        cache_str = dumps(
            {sub_id1: {"resource1": "setting1", "resource2": "setting2"}, sub_id2: {"resource3": "setting3"}}
        )
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)

    def test_missing_config_keys(self):
        cache_str = dumps({sub_id1: {"resource1": {}}})
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)

    def test_partial_missing_config_keys(self):
        cache_str = dumps({sub_id1: {"resource1": {"diagnostic_setting_id": "hi", "event_hub_name": "eh"}}})
        self.assertEqual(deserialize_diagnostic_settings_cache(cache_str), {})
        self.log.warning.assert_called_once_with(DIAGNOSTIC_SETTINGS_CACHE_ERROR_MSG)
