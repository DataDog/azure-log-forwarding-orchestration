from json import dumps
from unittest import TestCase

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

    def test_invalid_json(self):
        cache_str = "{invalid_json}"
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_not_dict(self):
        cache_str = dumps(["not_a_dict"])
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_dict_with_non_dict_regions(self):
        cache_str = dumps({sub_id1: "not_a_dict_region_config"})
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_dict_with_non_list_resources(self):
        cache_str = dumps({sub_id1: {"region": "not_a_list_of_resources"}})
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_dict_with_some_non_list_values(self):
        cache_str = dumps({sub_id1: {"region1": ["r1"]}, sub_id2: {"region2": {"hi": "value"}}})
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)
