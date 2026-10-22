# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# project
from cache.resources_cache import ResourceMetadata
from tasks.common import CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX
from tasks.tests.common import mock

sub_id1 = "a062baee-fdd3-4784-beb4-d817f591422c"

SUPPORTED_REGION_1 = "norwayeast"
SUPPORTED_REGION_2 = "southafricanorth"
CONTAINER_APPS_UNSUPPORTED_REGION = "newzealandnorth"
UNSUPPORTED_REGION = "uae"

resource1 = mock(
    id="res1", name="1", location=SUPPORTED_REGION_1, type="Microsoft.Network/loadBalancers", tags={"datadog": "true"}
)
resource2 = mock(
    id="res2", name="2", location=SUPPORTED_REGION_1, type="Microsoft.Network/applicationgateways", tags=None
)
keyvault_resource = mock(
    id="kv1", name="kv1", location=SUPPORTED_REGION_1, type="Microsoft.KeyVault/vaults", tags={"datadog": "true"}
)
eventhub_resource = mock(
    id="eh1", name="eh1", location=SUPPORTED_REGION_1, type="Microsoft.EventHub/namespaces", tags=None
)
control_plane_storage_account = mock(
    id="cpsa",
    name=f"{CONTROL_PLANE_STORAGE_ACCOUNT_PREFIX}abc",
    location=SUPPORTED_REGION_1,
    type="Microsoft.Storage/storageAccounts",
    tags={"datadog": "true"},
)

included_metadata = ResourceMetadata(include=True)
excluded_metadata = ResourceMetadata(include=False)
