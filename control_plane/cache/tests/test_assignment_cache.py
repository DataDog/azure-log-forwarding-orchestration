# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/  Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest import TestCase

# project
from cache.assignment_cache import AssignmentCache, deserialize_assignment_cache
from cache.common import EVENT_HUB_TYPE, STORAGE_ACCOUNT_TYPE
from cache.tests import sub_id1


class TestDeserializeResourceCache(TestCase):
    def test_valid_cache(self):
        raw_cache: AssignmentCache = {
            sub_id1: {
                "region2": {
                    "resources": {"resource1": "sjaksdhsj", "resource2": "iasudkajs", "resource3": "iasudkajs"},
                    "configurations": {
                        "iasudkajs": STORAGE_ACCOUNT_TYPE,
                        "sjaksdhsj": EVENT_HUB_TYPE,
                    },
                }
            },
        }
        cache = deserialize_assignment_cache(dumps(raw_cache))
        self.assertEqual(cache, raw_cache)

    def assert_deserialize_failure(self, cache_str: str):
        cache = deserialize_assignment_cache(cache_str)
        self.assertIsNone(cache)

    def test_invalid_json(self):
        self.assert_deserialize_failure("{invalid_json}")

    def test_not_dict(self):
        self.assert_deserialize_failure(dumps(["not_a_dict"]))

    def test_not_dict_regions(self):
        self.assert_deserialize_failure(dumps({sub_id1: "value"}))

    def test_incorrectly_shaped_region_config(self):
        self.assert_deserialize_failure(
            dumps(
                {
                    sub_id1: {
                        "region1": {"some": "garbage"},
                        "region2": {"resources": {}, "configurations": {}},
                    }
                }
            )
        )

    def test_invalid_resource_assignments(self):
        self.assert_deserialize_failure(
            dumps(
                {
                    sub_id1: {
                        "region1": {"resources": ["not_a_dict"], "configurations": {}},
                    }
                }
            )
        )

    def test_invalid_forwarder_configurations(self):
        self.assert_deserialize_failure(
            dumps(
                {
                    sub_id1: {
                        "region1": {
                            "resources": {"resource1": "config1"},
                            "configurations": {"config1": {"hmm": "not_a_config"}},
                        },
                    }
                }
            )
        )

    def test_partial_region_config(self):
        self.assert_deserialize_failure(
            dumps(
                {
                    sub_id1: {
                        "region1": {"resources": {"resource1": "config1"}},  # missing configurations field
                    }
                }
            )
        )

    def test_missing_config_key(self):
        cache: AssignmentCache = {
            sub_id1: {
                "region1": {"resources": {"resource1": "config1"}, "configurations": {"config2": EVENT_HUB_TYPE}}
            },
        }
        self.assert_deserialize_failure(dumps(cache))
