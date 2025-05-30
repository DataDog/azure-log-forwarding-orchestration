# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from json import dumps
from unittest import TestCase

# project
from cache.resources_cache import (
    INCLUDE_KEY,
    ResourceCache,
    ResourceMetadata,
    _deserialize_v1_resource_cache,
    _deserialize_v2_resource_cache,
    deserialize_monitored_subscriptions,
    deserialize_resource_cache,
    deserialize_resource_tag_filters,
    prune_resource_cache,
)
from cache.tests import sub_id1, sub_id2

included_metadata: ResourceMetadata = {INCLUDE_KEY: True}
excluded_metadata: ResourceMetadata = {INCLUDE_KEY: False}


class TestDeserializeResourceCache(TestCase):
    def test_read_cache_existing_v2(self):
        cache_str = dumps(
            {
                sub_id1: {"region2": {"resource1": included_metadata, "resource2": excluded_metadata}},
                sub_id2: {"region3": {"resource3": included_metadata}},
            }
        )
        cache = _deserialize_v2_resource_cache(cache_str)

        self.assertEqual(
            cache,
            {
                sub_id1: {"region2": {"resource1": included_metadata, "resource2": excluded_metadata}},
                sub_id2: {"region3": {"resource3": included_metadata}},
            },
        )

    def test_read_cache_existing_v1_upgrades(self):
        cache_str = dumps({sub_id1: {"region2": ["resource1", "resource2"]}})
        cache, should_flush = deserialize_resource_cache(cache_str)

        self.assertTrue(should_flush)
        self.assertEqual(
            cache,
            {sub_id1: {"region2": {"resource1": included_metadata, "resource2": included_metadata}}},
        )

    def test_v1_schema(self):
        cache_str = dumps({sub_id1: {"region2": ["resource1", "resource2"]}})
        self.assert_deserialize_v2_failure(cache_str)
        cache = _deserialize_v1_resource_cache(cache_str)
        self.assertEqual(cache, {sub_id1: {"region2": ["resource1", "resource2"]}})

    def assert_deserialize_v2_failure(self, cache_str: str):
        cache = _deserialize_v2_resource_cache(cache_str)
        self.assertIsNone(cache)

    def assert_deserialize_v1_failure(self, cache_str: str):
        cache = _deserialize_v1_resource_cache(cache_str)
        self.assertIsNone(cache)

    def test_invalid_json(self):
        self.assert_deserialize_v2_failure("{invalid_json}")
        self.assert_deserialize_v1_failure("{invalid_json}")

    def test_not_dict(self):
        self.assert_deserialize_v2_failure(dumps(["not_a_dict"]))
        self.assert_deserialize_v1_failure(dumps(["not_a_dict"]))

    def test_dict_with_non_dict_regions(self):
        self.assert_deserialize_v2_failure(dumps({sub_id1: "not_a_dict_region_config"}))
        self.assert_deserialize_v1_failure(dumps({sub_id1: "not_a_dict_region_config"}))

    def test_dict_with_non_list_resources(self):
        self.assert_deserialize_v2_failure(dumps({sub_id1: {"region": "not_a_list_of_resources"}}))
        self.assert_deserialize_v1_failure(dumps({sub_id1: {"region": "not_a_list_of_resources"}}))

    def test_dict_with_some_non_list_values(self):
        self.assert_deserialize_v2_failure(dumps({sub_id1: {"region1": ["r1"]}, sub_id2: {"region2": 123}}))
        self.assert_deserialize_v1_failure(dumps({sub_id1: {"region1": ["r1"]}, sub_id2: {"region2": 123}}))

    def test_prune_resources_cache_empty(self):
        cache: ResourceCache = {}
        prune_resource_cache(cache)
        self.assertEqual(cache, {})

    def test_prune_resources_cache_empty_subscription(self):
        cache: ResourceCache = {"sub1": {}, "sub2": {"region1": {"resource1": included_metadata}}}
        prune_resource_cache(cache)
        self.assertEqual(cache, {"sub2": {"region1": {"resource1": included_metadata}}})

    def test_prune_resources_cache_empty_region(self):
        cache: ResourceCache = {"sub1": {"region2": dict()}, "sub2": {"region1": {"resource1": included_metadata}}}
        prune_resource_cache(cache)
        self.assertEqual(cache, {"sub2": {"region1": {"resource1": included_metadata}}})

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

    def test_deserialize_resource_tag_filters(self):
        env_var = "datadog:true, env:STAGING,!env:pRoD        , !not:me,solo"
        tag_filters = deserialize_resource_tag_filters(env_var)
        self.assertEqual(tag_filters, ["datadog:true", "env:staging", "!env:prod", "!not:me", "solo"])

    def test_deserialize_empty_resource_tag_filters(self):
        tag_filters = deserialize_resource_tag_filters("")
        self.assertEqual(tag_filters, [])
