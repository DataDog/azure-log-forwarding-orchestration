# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# stdlib
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock

# project
from tasks.concurrency import collect, create_task_from_awaitable, safe_collect


class TestCollect(IsolatedAsyncioTestCase):
    async def test_collect_with_values(self):
        async def async_gen():
            for i in range(3):
                yield i

        result = await collect(async_gen())
        self.assertEqual(result, [0, 1, 2])

    async def test_collect_empty(self):
        async def async_gen():
            if False:
                yield

        result = await collect(async_gen())
        self.assertEqual(result, [])


class TestCreateTaskFromAwaitable(IsolatedAsyncioTestCase):
    async def test_create_task_from_awaitable(self):
        async def async_func():
            return 42

        task = create_task_from_awaitable(async_func())
        result = await task
        self.assertEqual(result, 42)

    async def test_create_task_from_awaitable_exception(self):
        async def async_func():
            raise ValueError("oops")

        task = create_task_from_awaitable(async_func())
        with self.assertRaises(ValueError) as ctx:
            await task
        self.assertEqual(str(ctx.exception), "oops")


class TestSafeCollect(IsolatedAsyncioTestCase):
    async def test_safe_collect_with_values(self):
        async def async_gen():
            for i in range(3):
                yield i

        result = await safe_collect(async_gen(), Mock())
        self.assertEqual(result, [0, 1, 2])

    async def test_safe_collect_empty(self):
        async def async_gen():
            if False:
                yield

        result = await safe_collect(async_gen(), Mock())
        self.assertEqual(result, [])

    async def test_safe_collect_with_exception(self):
        err = ValueError("oops")

        async def async_gen():
            yield 1
            raise err
            yield 2

        log = Mock()
        result = await safe_collect(async_gen(), log)
        self.assertEqual(result, [1])
        log.warning.assert_called_once_with("Ignored error while collecting async iterable: %s", err)
