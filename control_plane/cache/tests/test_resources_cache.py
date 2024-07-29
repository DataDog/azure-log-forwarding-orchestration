# stdlib
from json import dumps
from unittest import TestCase

# project
from cache.resources_cache import deserialize_resource_cache
from cache.tests import sub_id1, sub_id2


class TestDeserializeResourceCache(TestCase):
    def test_valid_cache(self):
        cache_str = dumps({sub_id1: {"region2": ["resource1", "resource2"]}, sub_id2: {"region3": ["resource3"]}})
        success, cache = deserialize_resource_cache(cache_str)
        self.assertTrue(success)
        self.assertEqual(
            cache,
            {sub_id1: {"region2": {"resource1", "resource2"}}, sub_id2: {"region3": {"resource3"}}},
        )

    def assert_deserialize_failure(self, cache_str: str):
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_invalid_json(self):
        self.assert_deserialize_failure("{invalid_json}")

    def test_not_dict(self):
        self.assert_deserialize_failure(dumps(["not_a_dict"]))

    def test_dict_with_non_dict_regions(self):
        self.assert_deserialize_failure(dumps({sub_id1: "not_a_dict_region_config"}))

    def test_dict_with_non_list_resources(self):
        self.assert_deserialize_failure(dumps({sub_id1: {"region": "not_a_list_of_resources"}}))

    def test_dict_with_some_non_list_values(self):
        self.assert_deserialize_failure(dumps({sub_id1: {"region1": ["r1"]}, sub_id2: {"region2": {"hi": "value"}}}))
