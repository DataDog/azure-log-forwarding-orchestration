# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from asyncio import sleep
from math import inf
from unittest import IsolatedAsyncioTestCase

from azure.core.exceptions import ResourceNotFoundError

# 3p
from azure.core.polling import AsyncLROPoller

# project
from tasks.common import (
    average,
    generate_unique_id,
    get_container_app_id,
    get_managed_env_id,
    get_resource_group_id,
    get_storage_account_id,
    now,
    resource_tag_dict_to_list,
)
from tasks.deploy_common import wait_for_resource

sub1 = "sub1"
rg1 = "rg1"
config1 = "config1"
region = "region"


class MockPoller(AsyncLROPoller):
    def __init__(self):
        pass

    async def result(self):
        return "result"


def make_check_resource(tries: int = 0):
    async def check_resource():
        nonlocal tries
        await sleep(0.01)  # network call
        if tries == 0:
            return
        tries -= 1
        raise ResourceNotFoundError("Resource not found")

    return check_resource


class TestCommon(IsolatedAsyncioTestCase):
    def test_now(self):
        self.assertRegex(now(), r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}")

    def test_average(self):
        self.assertEqual(average(1, 2, 3), 2)
        self.assertEqual(average(), inf)
        self.assertEqual(average(default=-1), -1)
        self.assertEqual(average(0), 0)
        self.assertAlmostEqual(average(1, 3, 4), 2.6666, places=3)

    async def test_wait_for_resource_no_wait(self):
        res = await wait_for_resource(MockPoller(), make_check_resource())
        self.assertEqual(res, "result")

    async def test_wait_for_resource_3_tries(self):
        res = await wait_for_resource(MockPoller(), make_check_resource(tries=3))
        self.assertEqual(res, "result")

    def test_generate_unique_id(self):
        for _ in range(10):  # test multiple times to ensure
            self.assertRegex(generate_unique_id(), r"[0-9a-f]{12}")

    def test_get_resource_group_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourcegroups/rg1",
            get_resource_group_id("sub1", "rg1"),
        )
        self.assertTrue(get_resource_group_id("UpperCaseSub", "SomeUpperCaseRG").islower())

    def test_get_container_app_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourcegroups/rg1/providers/microsoft.app/jobs/dd-log-forwarder-config1",
            get_container_app_id(sub1, rg1, config1),
        )
        self.assertTrue(get_container_app_id("UpperCaseSub", "SomeUpperCaseRG", config1).islower())

    def test_get_managed_env_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourcegroups/rg1/providers/microsoft.app/managedenvironments/dd-log-forwarder-env-config1-region",
            get_managed_env_id(sub1, rg1, region, config1),
        )
        self.assertTrue(get_managed_env_id("UpperCaseSub", "SomeUpperCaseRG", region, config1).islower())

    def test_get_storage_account_id(self):
        self.assertEqual(
            "/subscriptions/sub1/resourcegroups/rg1/providers/microsoft.storage/storageaccounts/ddlogstorageconfig1",
            get_storage_account_id(sub1, rg1, config1),
        )
        self.assertTrue(get_storage_account_id("UpperCaseSub", "SomeUpperCaseRG", config1).islower())

    def test_resource_tag_dict_to_list_values(self):
        self.assertEqual(
            ["environment:dev", "team:engineering"],
            resource_tag_dict_to_list({"environment": "dev", "team": "engineering"}),
        )

    def test_resource_tag_dict_to_list_normalized(self):
        self.assertEqual(
            ["environment:dev", "team:engineering"],
            resource_tag_dict_to_list({"Environment": " Dev", "TEAM": "engineering   "}),
        )

    def test_resource_tag_dict_to_list_keys_only(self):
        self.assertEqual(
            ["environment", "team"],
            resource_tag_dict_to_list({"environment": "", "team": ""}),
        )

    def test_resource_tag_dict_to_list_empty(self):
        self.assertEqual(
            [],
            resource_tag_dict_to_list({}),
        )

    def test_resource_tag_dict_to_list_none(self):
        self.assertEqual(
            [],
            resource_tag_dict_to_list(None),
        )
