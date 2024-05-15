from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, patch


class TaskTestCase(IsolatedAsyncioTestCase):
    TASK_NAME: str = NotImplemented

    def patch_path(self, path: str):
        p = patch(path)
        self.addCleanup(p.stop)
        return p.start()

    def patch(self, obj: str):
        return self.patch_path(f"src.tasks.{self.TASK_NAME}.{obj}")

    def setUp(self) -> None:
        self.credential = self.patch_path("src.tasks.common.DefaultAzureCredential")
        self.credential.side_effect = AsyncMock
        self.write_cache: AsyncMock = self.patch("write_cache")

    def cache_value(self, cache_name: str) -> str:
        self.write_cache.assert_called_with(cache_name, ANY)
        return self.write_cache.call_args_list[-1][0][1]
