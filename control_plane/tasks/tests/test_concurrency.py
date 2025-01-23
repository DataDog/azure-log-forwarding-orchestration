# stdlib
from unittest import IsolatedAsyncioTestCase

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

        result = await safe_collect(async_gen())
        self.assertEqual(result, [0, 1, 2])

    async def test_safe_collect_empty(self):
        async def async_gen():
            if False:
                yield

        result = await safe_collect(async_gen())
        self.assertEqual(result, [])

    async def test_safe_collect_with_exception(self):
        async def async_gen():
            yield 1
            raise ValueError("oops")
            yield 2

        result = await safe_collect(async_gen())
        self.assertEqual(result, [1])
