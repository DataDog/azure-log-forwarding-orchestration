# stdlib
from asyncio import sleep
from math import inf
from unittest import IsolatedAsyncioTestCase

from azure.core.exceptions import ResourceNotFoundError

# 3p
from azure.core.polling import AsyncLROPoller

# project
from tasks.common import average, now, wait_for_resource


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
