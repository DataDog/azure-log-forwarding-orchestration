from json import dumps, loads
from typing import Any, AsyncIterable, Callable, TypeAlias, TypeVar
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from function_app import ResourcesTask, ResourceCache, SubscriptionId
from unittest import IsolatedAsyncioTestCase


T = TypeVar("T")

AsyncIterableFunc: TypeAlias = Callable[[], AsyncIterable[Mock]]


async def agen(*items: T) -> AsyncIterable[T]:
    for x in items:
        yield x


def make_agen_func(field_name: str, *values: str) -> AsyncIterableFunc:
    """useful wrapper for client methods which return an `AsyncIterable` of objects with a single field."""
    return Mock(return_value=agen(*(Mock(**{field_name: value}) for value in values)))


class TestAzureDiagnosticSettingsCrawler(IsolatedAsyncioTestCase):
    def get_mock_client(self, client_name: str) -> AsyncMock:
        client_patch = patch(f"function_app.{client_name}")
        self.addCleanup(client_patch.stop)
        return client_patch.start()

    def setUp(self) -> None:
        self.sub_client: AsyncMock = self.get_mock_client("SubscriptionClient").return_value.__aenter__.return_value
        self.resource_client = self.get_mock_client("ResourceManagementClient")
        self.resource_client_mapping: dict[SubscriptionId, AsyncIterableFunc] = {}

        def create_resource_client(_: Any, sub_id: SubscriptionId):
            c = MagicMock()
            c.__aenter__.return_value.resources.list = self.resource_client_mapping[sub_id]
            return c

        self.resource_client.side_effect = create_resource_client
        self.credential = AsyncMock()
        self.out_mock = Mock()

    @property
    def out_value(self) -> ResourceCache:
        return loads(self.out_mock.set.call_args[0][0])

    async def test_resources_crawler_empty_cache_adds_resources(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }

        await ResourcesTask(self.credential, "", self.out_mock).run()

        self.assertEqual(
            self.out_value,
            {
                "sub1": {"res1": {}, "res2": {}},
                "sub2": {"res3": {}},
            },
        )

    async def test_resources_crawler_no_new_resources_doesnt_cache(self):
        self.sub_client.subscriptions.list = make_agen_func("subscription_id", "sub1", "sub2")
        self.resource_client_mapping = {
            "sub1": make_agen_func("id", "res1", "res2"),
            "sub2": make_agen_func("id", "res3"),
        }

        await ResourcesTask(
            self.credential,
            dumps(
                {
                    "sub1": {"res1": {}, "res2": {}},
                    "sub2": {"res3": {}},
                }
            ),
            self.out_mock,
        ).run()

        self.out_mock.set.assert_not_called()
