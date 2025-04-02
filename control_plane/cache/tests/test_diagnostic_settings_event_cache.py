# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest import TestCase

# project
from cache.diagnostic_settings_cache import (
    DIAGNOSTIC_SETTINGS_COUNT,
    SENT_EVENT,
    DiagnosticSettingsCache,
    deserialize_event_cache,
    remove_cached_resource,
    update_cached_event,
)
from cache.tests import sub_id1, sub_id2


class TestDeserializeDiagnosticSettingsCache(TestCase):
    def test_valid_cache(self):
        valid_cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False},
                "resource2_id": {DIAGNOSTIC_SETTINGS_COUNT: 5, SENT_EVENT: True},
            },
            sub_id2: {
                "resource3_id": {DIAGNOSTIC_SETTINGS_COUNT: 5, SENT_EVENT: False},
            },
        }
        cache_str = dumps(valid_cache)
        test_cache = deserialize_event_cache(cache_str)
        self.assertEqual(
            test_cache,
            valid_cache,
        )

    def assert_deserialize_failure(self, cache_str: str):
        cache = deserialize_event_cache(cache_str)
        self.assertIsNone(cache)

    def test_invalid_json_is_invalid(self):
        self.assert_deserialize_failure("{invalid_json}")

    def test_not_dict_cache_is_invalid(self):
        self.assert_deserialize_failure(dumps(["not_a_dict"]))

    def test_non_dict_resources_config_is_invalid(self):
        self.assert_deserialize_failure(dumps({sub_id1: "not_a_dict"}))

    def test_non_dict_event_config_is_invalid(self):
        self.assert_deserialize_failure(dumps({sub_id1: {"resource1_id": "not_a_dict"}}))

    def test_wrong_count_type_event_config_is_invalid(self):
        self.assert_deserialize_failure(dumps({sub_id1: {"resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: "not_an_int"}}}))

    def test_wrong_alert_type_event_config_is_invalid(self):
        self.assert_deserialize_failure(dumps({sub_id1: {"resource1_id": {SENT_EVENT: "not_a_bool"}}}))

    def test_remove_cached_resource(self):
        cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False},
                "resource2_id": {DIAGNOSTIC_SETTINGS_COUNT: 2, SENT_EVENT: False},
            },
        }
        remove_cached_resource(cache, sub_id1, "resource2_id")
        self.assertEqual(cache, {sub_id1: {"resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False}}})

    def test_remove_cached_resource_missing_sub_id(self):
        cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False},
                "resource2_id": {DIAGNOSTIC_SETTINGS_COUNT: 2, SENT_EVENT: False},
            },
        }
        remove_cached_resource(cache, sub_id2, "resource2_id")
        self.assertEqual(
            cache,
            {
                sub_id1: {
                    "resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False},
                    "resource2_id": {DIAGNOSTIC_SETTINGS_COUNT: 2, SENT_EVENT: False},
                }
            },
        )

    def test_update_cached_setting(self):
        cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False},
            },
        }
        update_cached_event(cache, sub_id1, "resource1_id", 2, False)
        self.assertEqual(cache, {sub_id1: {"resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 2, SENT_EVENT: False}}})

    def test_update_cached_setting_count_new_sub_id(self):
        cache: DiagnosticSettingsCache = {
            sub_id1: {
                "resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False},
            },
        }
        update_cached_event(cache, sub_id2, "resource2_id", 5, True)
        self.assertEqual(
            cache,
            {
                sub_id1: {"resource1_id": {DIAGNOSTIC_SETTINGS_COUNT: 1, SENT_EVENT: False}},
                sub_id2: {"resource2_id": {DIAGNOSTIC_SETTINGS_COUNT: 5, SENT_EVENT: True}},
            },
        )
