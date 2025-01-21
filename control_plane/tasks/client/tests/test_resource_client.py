from unittest import IsolatedAsyncioTestCase, TestCase

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


class TestQueryFilter(TestCase):
    def test_resource_query_filter(self):
        for rt in FETCHED_RESOURCE_TYPES:
            self.assertTrue(f"resourceType eq '{rt}'" in RESOURCE_QUERY_FILTER)

        self.assertEqual(
            RESOURCE_QUERY_FILTER.count("or resourceType"),
            len(FETCHED_RESOURCE_TYPES) - 1,
        )


class TestShouldIgnoreResource(TestCase):
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
    def setUp(self) -> None:
        super().setUp()
        self.cred = mock()
        self.client = ResourceClient(self.cred, sub_id1)
        self.client.resources_client = AsyncMockClient()

    async def test_global_resource_ignored(self):
        self.client.resources_client.resources.list = mock(
            return_value=async_generator(
                mock(id="res1", location="global", type="Microsoft.Compute/virtualMachines"),
                mock(id="res2", location="global", type="Microsoft.Network/applicationGateways"),
            )
        )
        async with self.client:
            resources = await self.client.get_resources_per_region()
        self.assertEqual(resources, {})

    async def test_unsupported_resource_types_ignored(self):
        self.client.resources_client.resources.list = mock(
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
        async with self.client:
            resources = await self.client.get_resources_per_region()
        self.assertEqual(resources, {SUPPORTED_REGION_1: {"res2"}})

    async def test_lfo_resource_is_ignored(self):
        self.client.resources_client.resources.list = mock(
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

        async with self.client:
            resources = await self.client.get_resources_per_region()
        self.assertEqual(resources, {SUPPORTED_REGION_1: {"res1"}})
