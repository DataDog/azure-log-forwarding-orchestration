# stdlib
from json import dumps
from unittest import TestCase

# project
from cache.resources_cache import (
    ResourceCache,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    prune_resource_cache,
)
from cache.tests import sub_id1, sub_id2


class TestDeserializeResourceCache(TestCase):
    def test_valid_cache(self):
        cache_str = dumps({sub_id1: {"region2": ["resource1", "resource2"]}, sub_id2: {"region3": ["resource3"]}})
        cache = deserialize_resource_cache(cache_str)

        self.assertEqual(
            cache,
            {sub_id1: {"region2": {"resource1", "resource2"}}, sub_id2: {"region3": {"resource3"}}},
        )

    def assert_deserialize_failure(self, cache_str: str):
        cache = deserialize_resource_cache(cache_str)
        self.assertIsNone(cache)

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

    def test_prune_resources_cache_empty(self):
        cache: ResourceCache = {}
        prune_resource_cache(cache)
        self.assertEqual(cache, {})

    def test_prune_resources_cache_empty_subscription(self):
        cache: ResourceCache = {"sub1": {}, "sub2": {"region1": {"resource1"}}}
        prune_resource_cache(cache)
        self.assertEqual(cache, {"sub2": {"region1": {"resource1"}}})

    def test_prune_resources_cache_empty_region(self):
        cache: ResourceCache = {"sub1": {"region2": set()}, "sub2": {"region1": {"resource1"}}}
        prune_resource_cache(cache)
        self.assertEqual(cache, {"sub2": {"region1": {"resource1"}}})

    def test_monitored_subscriptions_var(self):
        env_var = '["8c56d827-5f07-45ce-8f2b-6c5001db5c6f","0b62a232-b8db-4380-9da6-640f7272ed6d"]'
        monitored_subscriptions = deserialize_monitored_subscriptions(env_var)
        self.assertEqual(
            monitored_subscriptions,
            [
                "8c56d827-5f07-45ce-8f2b-6c5001db5c6f",
                "0b62a232-b8db-4380-9da6-640f7272ed6d",
            ],
        )
