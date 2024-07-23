from json import dumps
from unittest import TestCase

from cache.assignment_cache import AssignmentCache, deserialize_assignment_cache

from cache.tests import sub_id1


class TestDeserializeResourceCache(TestCase):
    def test_valid_cache(self):
        raw_cache: AssignmentCache = {
            sub_id1: {
                "region2": {
                    "resources": {"resource1": "sjaksdhsj", "resource2": "iasudkajs", "resource3": "iasudkajs"},
                    "configurations": {
                        "iasudkajs": {
                            "type": "storageaccount",
                            "id": "iasudkajs",
                            "storage_account_id": "some/storage/account",
                        },
                        "sjaksdhsj": {
                            "type": "eventhub",
                            "id": "sjaksdhsj",
                            "event_hub_name": "eventhub",
                            "event_hub_namespace": "namespace",
                        },
                    },
                }
            },
        }
        success, cache = deserialize_assignment_cache(dumps(raw_cache))
        self.assertTrue(success)
        self.assertEqual(cache, raw_cache)

    def assert_deserialize_failure(self, cache_str: str):
        success, _ = deserialize_assignment_cache(cache_str)
        self.assertFalse(success)

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
                        "region1": {"resources": {"resource1": "config1"}},  # missing configurations
                    }
                }
            )
        )
