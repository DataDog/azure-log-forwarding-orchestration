from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from tasks.client.resource_client import RESOURCE_QUERY_FILTER, ResourceClient, should_ignore_resource
from tasks.constants import FETCHED_RESOURCE_TYPES
from tasks.tests.common import AsyncMockClient, async_generator, mock

sub_id1 = "a062baee-fdd3-4784-beb4-d817f591422c"
sub_id2 = "77602a31-36b2-4417-a27c-9071107ca3e6"
sub1 = mock(subscription_id=sub_id1)
sub2 = mock(subscription_id=sub_id2)

SUPPORTED_REGION_1 = "norwayeast"
SUPPORTED_REGION_2 = "southafricanorth"
CONTAINER_APPS_UNSUPPORTED_REGION = "newzealandnorth"
UNSUPPORTED_REGION = "uae"
resource1 = mock(id="res1", name="1", location=SUPPORTED_REGION_1, type="Microsoft.Compute/virtualMachines")
resource2 = mock(id="res2", name="2", location=SUPPORTED_REGION_1, type="Microsoft.Network/applicationgateways")
resource3 = mock(id="res3", name="3", location=SUPPORTED_REGION_2, type="Microsoft.Network/loadBalancers")


class TestResourceClientHelpers(TestCase):
    def test_resource_query_filter(self):
        for rt in FETCHED_RESOURCE_TYPES:
            self.assertTrue(f"resourceType eq '{rt}'" in RESOURCE_QUERY_FILTER)

        self.assertEqual(
            RESOURCE_QUERY_FILTER.count("or resourceType"),
            len(FETCHED_RESOURCE_TYPES) - 1,
        )

    def test_should_ignore_resource_by_region(self):
        vm_type = "Microsoft.Compute/virtualMachines"
        vm_name = "vm1"
        # valid regions
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, vm_type, vm_name))
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_2, vm_type, vm_name))
        self.assertFalse(should_ignore_resource(CONTAINER_APPS_UNSUPPORTED_REGION, vm_type, vm_name))

        # invalid regions
        self.assertTrue(should_ignore_resource(UNSUPPORTED_REGION, vm_type, vm_name))
        self.assertTrue(should_ignore_resource("nonsense region that doenst exist", vm_type, vm_name))
        self.assertTrue(should_ignore_resource("global", vm_type, vm_name))

    def test_should_ignore_resource_by_type(self):
        # valid types
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/virtualMachines", "vm1"))
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Network/applicationGateways", "ag1"))

        # invalid types
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/Snapshots", "snap1"))
        self.assertTrue(
            should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.AlertsManagement/PrometheusRuleGroups", "prg1")
        )

    def test_should_ignore_resource_by_name(self):
        # normal resources
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/virtualMachines", "vm1"))
        self.assertFalse(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Compute/virtualMachines", "vm2"))

        # TODO (AZINTS-2763): ensure storage accounts are ignored
        # control plane resources

        self.assertTrue(
            should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.App/managedEnvironments", "dd-log-forwarder-env-")
        )
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Web/sites", "scaling-task-"))
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Web/sites", "resources-task-"))
        self.assertTrue(should_ignore_resource(SUPPORTED_REGION_1, "Microsoft.Web/sites", "diagnostic-settings-task-"))


class TestResourceClient(IsolatedAsyncioTestCase):
    MOCKED_CLIENTS = ("ResourceManagementClient", "WebSiteManagementClient", "SqlManagementClient")

    def setUp(self) -> None:
        super().setUp()
        self.cred = mock()
        self.mock_clients = {}
        for client in self.MOCKED_CLIENTS:
            c = AsyncMockClient()
            p = patch(f"tasks.client.resource_client.{client}", return_value=c)
            p.start()
            self.addCleanup(p.stop)
            self.mock_clients[client] = c

    async def test_clients_mocked_properly(self):
        resource_client = ResourceClient(self.cred, sub_id1)
        for client, _ in resource_client._get_sub_resources_map.values():
            if client is None:
                continue
            self.assertIsInstance(client, AsyncMock)  # everything should be mocked

        async with resource_client as client:
            for client in self.MOCKED_CLIENTS:
                self.mock_clients[client].__aenter__.assert_awaited_once()

        for client in self.MOCKED_CLIENTS:
            self.mock_clients[client].__aexit__.assert_awaited_once()

    async def test_global_resource_ignored(self):
        self.mock_clients["ResourceManagementClient"].resources.list = mock(
            return_value=async_generator(
                mock(id="res1", location="global", type="Microsoft.Compute/virtualMachines"),
                mock(id="res2", location="global", type="Microsoft.Network/applicationGateways"),
            )
        )
        async with ResourceClient(self.cred, sub_id1) as client:
            resources = await client.get_resources_per_region()
        self.assertEqual(resources, {})

    async def test_unsupported_resource_types_ignored(self):
        self.mock_clients["ResourceManagementClient"].resources.list = mock(
            return_value=async_generator(
                mock(id="res1", location=SUPPORTED_REGION_1, type="Microsoft.Compute/Snapshots"),
                resource2,
                mock(
                    id="res3",
                    location=SUPPORTED_REGION_2,
                    type="Microsoft.AlertsManagement/PrometheusRuleGroups",
                ),
            )
        )
        async with ResourceClient(self.cred, sub_id1) as client:
            resources = await client.get_resources_per_region()
        self.assertEqual(resources, {SUPPORTED_REGION_1: {"res2"}})

    async def test_lfo_resource_is_ignored(self):
        self.mock_clients["ResourceManagementClient"].resources.list = mock(
            return_value=async_generator(
                mock(
                    id="/subscriptions/whatever/whatever/dd-lfo-control-12983471",
                    name="scaling-task-12983471",
                    location=SUPPORTED_REGION_1,
                    type="Microsoft.Web/sites",
                ),
                resource1,
            )
        )

        async with ResourceClient(self.cred, sub_id1) as client:
            resources = await client.get_resources_per_region()
        self.assertEqual(resources, {SUPPORTED_REGION_1: {"res1"}})

    async def test_storage_account_sub_resources_collected(self):
        self.mock_clients["ResourceManagementClient"].resources.list = mock(
            return_value=async_generator(
                mock(
                    id="/subscriptions/WHATEVER/whatever/some-storage-account",
                    name="some-storage-account",
                    location=SUPPORTED_REGION_1,
                    type="MICROSOFT.STORAGE/STORAGEACCOUNTS",
                ),
                resource1,
            )
        )

        async with ResourceClient(self.cred, sub_id1) as client:
            resources = await client.get_resources_per_region()

        self.assertEqual(
            resources,
            {
                SUPPORTED_REGION_1: {
                    "res1",
                    "/subscriptions/whatever/whatever/some-storage-account/blobservices/default",
                    "/subscriptions/whatever/whatever/some-storage-account/fileservices/default",
                    "/subscriptions/whatever/whatever/some-storage-account/queueservices/default",
                    "/subscriptions/whatever/whatever/some-storage-account/tableservices/default",
                }
            },
        )

    async def test_sql_managedinstances_collected(self):
        self.mock_clients["ResourceManagementClient"].resources.list = mock(
            return_value=async_generator(
                mock(
                    id="/subscriptions/WHATEVER/resourceGroups/my-rg/whatever/some-sql-managed-instance",
                    name="some-sql-managed-instance",
                    resource_group="my-rg",
                    location=SUPPORTED_REGION_1,
                    type="MICROSOFT.SQL/managedinstances",
                ),
                resource1,
            )
        )
        self.mock_clients["SqlManagementClient"].managed_databases.list_by_instance = mock(
            return_value=async_generator(
                mock(
                    value=[
                        mock(id="/subscriptions/.../db1", name="db1"),
                        mock(id="/subscriptions/.../db2", name="db2"),
                    ]
                )
            )
        )

        async with ResourceClient(self.cred, sub_id1) as client:
            resources = await client.get_resources_per_region()
        self.assertEqual(
            resources,
            {
                SUPPORTED_REGION_1: {
                    "/subscriptions/whatever/resourcegroups/my-rg/whatever/some-sql-managed-instance",
                    "/subscriptions/.../db2",
                    "/subscriptions/.../db1",
                    "res1",
                }
            },
        )
