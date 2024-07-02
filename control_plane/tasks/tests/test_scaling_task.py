from tasks.scaling_task import SCALING_TASK_NAME
from tasks.tests.common import TaskTestCase


class TestScalingTask(TaskTestCase):
    TASK_NAME = SCALING_TASK_NAME

    def setUp(self) -> None:
        super().setUp()
        # self.sub_client: AsyncMock = self.patch("SubscriptionClient").return_value.__aenter__.return_value
        # self.resource_client = self.patch("ResourceManagementClient")
        # self.resource_client_mapping: dict[str, AsyncIterableFunc] = {}

        # def create_resource_client(_: Any, sub_id: str):
        #     c = MagicMock()
        #     c.__aenter__.return_value.resources.list = self.resource_client_mapping[sub_id]
        #     return c

        # self.resource_client.side_effect = create_resource_client

        self.log = self.patch("log")
