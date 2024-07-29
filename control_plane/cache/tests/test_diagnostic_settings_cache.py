# stdlib
from json import dumps
from unittest import TestCase

# project
from cache.diagnostic_settings_cache import (
    DiagnosticSettingsCache,
    deserialize_diagnostic_settings_cache,
)
from cache.tests import sub_id1, sub_id2


class TestDeserializeDiagnosticSettingsCache(TestCase):
    def test_valid_cache(self):
        diagnostic_settings_cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1": "hi",
                "resource2": "1234",
            },
            sub_id2: {"resource3": "5678"},
        }
        cache_str = dumps(diagnostic_settings_cache)
        success, cache = deserialize_diagnostic_settings_cache(cache_str)
        self.assertTrue(success)
        self.assertEqual(
            cache,
            diagnostic_settings_cache,
        )

    def assert_deserialize_failure(self, cache_str: str):
        success, _ = deserialize_diagnostic_settings_cache(cache_str)
        self.assertFalse(success)

    def test_invalid_json_is_invalid(self):
        self.assert_deserialize_failure("{invalid_json}")

    def test_not_dict_cache_is_invalid(self):
        self.assert_deserialize_failure(dumps(["not_a_dict"]))

    def test_non_dict_resources_config_is_invalid(self):
        self.assert_deserialize_failure(dumps({sub_id1: "not_a_dict"}))

    def test_some_non_str_config_id(self):
        self.assert_deserialize_failure(
            dumps(
                {
                    sub_id1: {"resource1": "1234"},
                    sub_id2: {"resource2": ["invalid_config_id"]},
                }
            )
        )
