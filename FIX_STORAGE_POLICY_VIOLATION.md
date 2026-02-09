# Fix Azure Policy Violation in Storage Account Creation

## Prompt for Claude Code Agent

Please implement a fix for the Azure Policy violation that's preventing the scaling task from creating storage accounts for log forwarders.

---

## Problem Statement

The scaling task is failing with the following error when attempting to create storage accounts:

```
azure.core.exceptions.HttpResponseError: (RequestDisallowedByPolicy) Resource 'ddlogstoragefb151be05a70' was disallowed by policy.
```

**Two Azure Policies are being violated:**

1. **Policy: "Secure transfer to storage accounts should be enabled"**
   - Missing property: `supportsHttpsTrafficOnly = true`
   - Effect: Deny

2. **Policy: "Storage accounts should prevent anonymous blob access"**
   - Missing property: `allowBlobPublicAccess = false`
   - Effect: Deny

---

## Root Cause

The storage account creation code in the scaling task is missing two required security parameters that Azure Policy enforces at the management group level.

---

## Files to Modify

### 1. Primary Fix

**File:** `control_plane/tasks/client/log_forwarder_client.py`
**Method:** `create_log_forwarder_storage_account` (lines 262-284)

**Current Code:**
```python
async def create_log_forwarder_storage_account(
    self, region: str, storage_account_name: str
) -> ResourcePoller[StorageAccount]:
    self.log.info(
        "Creating storage account %s for region %s",
        storage_account_name,
        region,
        extra=self.log_extra,
    )
    return await self.storage_client.storage_accounts.begin_create(
        self.resource_group,
        storage_account_name,
        StorageAccountCreateParameters(
            sku=Sku(
                # TODO (AZINTS-2646): figure out which SKU we should be using here
                name="Standard_LRS"
            ),
            kind="StorageV2",
            location=region,
            public_network_access=PublicNetworkAccess.ENABLED,
            minimum_tls_version="TLS1_2",
        ),
    ), lambda: self.storage_client.storage_accounts.get_properties(self.resource_group, storage_account_name)
```

**Required Change:**
Add these two parameters to the `StorageAccountCreateParameters` object:
- `enable_https_traffic_only=True`
- `allow_blob_public_access=False`

**Expected Result:**
```python
        StorageAccountCreateParameters(
            sku=Sku(
                # TODO (AZINTS-2646): figure out which SKU we should be using here
                name="Standard_LRS"
            ),
            kind="StorageV2",
            location=region,
            public_network_access=PublicNetworkAccess.ENABLED,
            minimum_tls_version="TLS1_2",
            enable_https_traffic_only=True,    # ← ADD THIS LINE
            allow_blob_public_access=False,    # ← ADD THIS LINE
        ),
```

### 2. Test Update

**File:** `control_plane/tasks/client/tests/test_log_forwarder_client.py`
**Location:** Lines 152-160 (in the `test_create_log_forwarder_storage_account` test)

**Current Test Expectation:**
```python
AzureModelMatcher(
    {
        "sku": {"name": "Standard_LRS"},
        "kind": "StorageV2",
        "location": EAST_US,
        "public_network_access": "Enabled",
        "minimum_tls_version": "TLS1_2",
    }
)
```

**Required Change:**
Add the two new properties to the expected model:

```python
AzureModelMatcher(
    {
        "sku": {"name": "Standard_LRS"},
        "kind": "StorageV2",
        "location": EAST_US,
        "public_network_access": "Enabled",
        "minimum_tls_version": "TLS1_2",
        "enable_https_traffic_only": True,    # ← ADD THIS LINE
        "allow_blob_public_access": False,    # ← ADD THIS LINE
    }
)
```

---

## Technical Context

### SDK Information
- **Package:** `azure-mgmt-storage==22.2.0`
- **API Version:** `v2024_01_01`
- **Import Location:** Line 35 of `log_forwarder_client.py`

### Parameter Details
Both parameters are of type `bool | None`:
- `enable_https_traffic_only`: When `True`, requires all requests to use HTTPS
- `allow_blob_public_access`: When `False`, prevents anonymous public access to blobs

### Naming Convention
The Python SDK uses snake_case, while Azure ARM uses camelCase:
- Azure ARM: `supportsHttpsTrafficOnly` → Python SDK: `enable_https_traffic_only`
- Azure ARM: `allowBlobPublicAccess` → Python SDK: `allow_blob_public_access`

---

## Reference Implementation

The Terraform module correctly configures the control plane storage account with these properties.

**File:** `/Users/matt.spurlin/go/src/github.com/DataDog/terraform-azurerm-log-forwarding-datadog/modules/automation/main.tf` (lines 77-95)

```hcl
resource "azurerm_storage_account" "control_plane" {
  name                            = local.resource_names.storage_account
  resource_group_name             = azurerm_resource_group.resource_group.name
  location                        = azurerm_resource_group.resource_group.location
  account_tier                    = "Standard"
  account_replication_type        = var.storage_replication_type
  account_kind                    = "StorageV2"
  access_tier                     = "Hot"
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true     # ← Corresponds to enable_https_traffic_only
  allow_nested_items_to_be_public = false    # ← Corresponds to allow_blob_public_access

  blob_properties {
    change_feed_enabled = false
    versioning_enabled  = false
  }

  tags = var.tags
}
```

---

## Verification Steps

After making the changes, please:

1. **Run the tests:**
   ```bash
   cd control_plane/tasks/client
   pytest tests/test_log_forwarder_client.py::test_create_log_forwarder_storage_account -v
   ```

2. **Verify the entire test suite passes:**
   ```bash
   pytest tests/test_log_forwarder_client.py -v
   ```

3. **Check for any other references to storage account creation:**
   ```bash
   grep -rn "StorageAccountCreateParameters" control_plane/
   ```
   (Should only find the one instance we're fixing)

4. **Build the tasks** (if tests pass):
   ```bash
   ./ci/scripts/control_plane/build_tasks.sh
   ```

---

## Impact & Context

### What This Fixes
- The scaling task will be able to create storage accounts for log forwarders
- New log forwarder instances can be deployed to new regions
- Compliance with Azure security policies enforced at the management group level

### Why These Properties Are Required
- **HTTPS-only traffic:** Ensures all data in transit is encrypted
- **No anonymous blob access:** Prevents unauthorized public access to storage data
- **Azure Policy enforcement:** These are organization-wide security requirements

### Deployment Process
After fixing and building:
1. The build process creates ZIP files for each task (including `scaling_task.zip`)
2. The CI/CD pipeline uploads these ZIPs to `https://ddazurelfo.blob.core.windows.net`
3. The deployer container app job (running every 30 minutes) automatically detects and deploys the new version to the scaling task function app

---

## Additional Notes

- This is the **only location** in the codebase where storage accounts are created dynamically
- Existing log forwarders continue to work; this only affects the creation of **new** storage accounts
- The control plane storage account (created by Terraform) already has these properties set correctly
- No changes to requirements.txt or other dependencies are needed

---

## Success Criteria

✅ Code changes made to `log_forwarder_client.py`
✅ Test expectations updated in `test_log_forwarder_client.py`
✅ All tests pass
✅ Build completes successfully
✅ No other instances of `StorageAccountCreateParameters` found

Once complete, the scaling task should successfully create storage accounts without policy violations.
