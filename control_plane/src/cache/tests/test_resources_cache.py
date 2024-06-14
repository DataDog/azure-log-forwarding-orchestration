from json import dumps
from unittest import TestCase

from src.cache.resources_cache import deserialize_resource_cache

from src.cache.tests import sub_id1, sub_id2


class TestDeserializeResourceCache(TestCase):
    def test_valid_cache(self):
        cache_str = dumps({sub_id1: ["resource1", "resource2"], sub_id2: ["resource3"]})
        success, cache = deserialize_resource_cache(cache_str)
        self.assertTrue(success)
        self.assertEqual(
            cache,
            {sub_id1: frozenset(["resource1", "resource2"]), sub_id2: frozenset(["resource3"])},
        )

    def test_invalid_json(self):
        cache_str = "{invalid_json}"
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_not_dict(self):
        cache_str = dumps(["not_a_dict"])
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_dict_with_non_list_values(self):
        cache_str = dumps({sub_id1: "not_a_list"})
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)

    def test_dict_with_some_non_list_values(self):
        cache_str = dumps({sub_id1: ["r1"], sub_id2: {"hi": "value"}})
        success, _ = deserialize_resource_cache(cache_str)
        self.assertFalse(success)
