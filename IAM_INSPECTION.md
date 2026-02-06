# IAM Permissions Inspection Guide

This guide explains how to view and inspect the IAM (Identity and Access Management) permissions created by the Azure Log Forwarding Orchestration deployment using the Azure Portal.

## Overview

The orchestration system creates several managed identities and role assignments to enable automated resource discovery, diagnostic settings configuration, and forwarder deployment across your Azure subscriptions. This guide will help you verify and understand these permissions.

### IAM Resources Created

- **4 System-Assigned Managed Identities** (Function Apps and Container App Job)
- **1 User-Assigned Managed Identity** (Bicep deployments only - for initial setup)
- **1 Custom Role Definition** (Bicep deployments only - ContainerAppStartRole)
- **7-8 Role Assignments per monitored subscription** (across subscription, resource group, and resource scopes)

---

## Prerequisites

To view IAM permissions in the Azure Portal, you need one of the following roles:

- **Owner** or **User Access Administrator** - Full visibility of all role assignments
- **Reader** role at the appropriate scope - Read-only view of role assignments
- **Security Reader** - View permissions for security auditing

If you cannot see IAM information, contact your Azure administrator to grant you the appropriate permissions.

---

## 1. Viewing Managed Identities

Managed identities are used by Azure resources to authenticate to other Azure services without storing credentials.

### View System-Assigned Identities (Function Apps)

**Navigation Path:**
```
Azure Portal → Resource Groups → [Control Plane Resource Group] → [Function App] → Identity
```

**Step-by-step:**

1. Navigate to the **Azure Portal** (https://portal.azure.com)
2. Select **Resource Groups** from the left sidebar
3. Find and click your **control plane resource group** (e.g., `ddlfo-control-plane-<controlPlaneId>`)
4. You'll see three Function Apps:
   - `resources-task-<controlPlaneId>`
   - `diagnostic-settings-task-<controlPlaneId>`
   - `scaling-task-<controlPlaneId>`
5. Click on any Function App name
6. In the left sidebar under **Settings**, click **Identity**
7. On the **System assigned** tab, you'll see:
   - **Status:** On
   - **Object (principal) ID:** The unique identifier for this managed identity
   - **Azure role assignments:** Click to see all roles assigned to this identity

**What to look for:**
- Status should be **On** (enabled)
- Principal ID should be a GUID (e.g., `a1b2c3d4-e5f6-7890-abcd-ef1234567890`)
- Role assignments section shows the permissions granted

### View System-Assigned Identity (Container App Job)

**Navigation Path:**
```
Azure Portal → Resource Groups → [Control Plane Resource Group] → [Container App Job] → Identity
```

**Step-by-step:**

1. In your control plane resource group, find the Container App Job:
   - `deployer-task-<controlPlaneId>`
2. Click on the Container App Job name
3. In the left sidebar under **Settings**, click **Identity**
4. View the **System assigned** identity details

### View User-Assigned Identity (Bicep Deployments Only)

**Navigation Path:**
```
Azure Portal → Resource Groups → [Control Plane Resource Group] → [User Assigned Identity]
```

**Step-by-step:**

1. In your control plane resource group, look for the **Managed Identity** resource:
   - `initialRunIdentity<controlPlaneId>`
2. Click on the identity name
3. On the **Overview** page, you'll see:
   - **Resource ID**
   - **Client ID**
   - **Principal ID** (Object ID)
4. Click **Azure role assignments** in the left sidebar to see its permissions

---

## 2. Viewing Role Assignments

Role assignments grant managed identities permissions to access Azure resources. Permissions are assigned at three different scopes: subscription, resource group, and resource level.

### Method A: View from the Managed Identity

This method shows all roles assigned to a specific identity across all scopes.

**Navigation Path:**
```
Azure Portal → Resource → Identity → Azure role assignments
```

**Step-by-step:**

1. Navigate to any Function App or Managed Identity (as described in Section 1)
2. Go to **Identity** → **System assigned** (or view the User Assigned Identity)
3. Click **Azure role assignments** button
4. You'll see a table with:
   - **Scope:** Where the permission applies (subscription, resource group, or resource)
   - **Role:** The permission level (e.g., Monitoring Reader, Contributor)
   - **Status:** Whether the assignment is active

**Expected role assignments by identity:**

| Identity | Expected Roles | Scope |
|----------|---------------|-------|
| **resources-task** | Monitoring Reader | Each monitored subscription |
| **resources-task** | Storage Blob Data Contributor (Terraform only) | Control plane storage account |
| **diagnostic-settings-task** | Monitoring Contributor | Each monitored subscription |
| **diagnostic-settings-task** | Reader and Data Access | Each monitored resource group |
| **scaling-task** | Contributor | Each monitored resource group |
| **deployer-task** | Monitoring Contributor | Each monitored subscription |
| **deployer-task** | Contributor | Each monitored resource group |
| **deployer-task** | Website Contributor | Control plane resource group |
| **initialRunIdentity** (Bicep) | Monitoring Contributor | Each monitored subscription |
| **initialRunIdentity** (Bicep) | Contributor | Each monitored resource group |
| **initialRunIdentity** (Bicep) | ContainerAppStartRole | Control plane resource group |

### Method B: View from Subscription IAM

This method shows all role assignments within a subscription, useful for auditing who has access.

**Navigation Path:**
```
Azure Portal → Subscriptions → [Subscription] → Access control (IAM) → Role assignments
```

**Step-by-step:**

1. Navigate to **Subscriptions** in the Azure Portal
2. Select the **monitored subscription** you want to inspect
3. In the left sidebar, click **Access control (IAM)**
4. Click the **Role assignments** tab
5. Use the search box or filters to find your identities:
   - Search by identity name (e.g., `resources-task`)
   - Filter by **Type:** Managed Identity
   - Look for the description containing your `controlPlaneId` or `ddlfo`

**What to look for:**

At the subscription level, you should see:
- **Monitoring Reader** assigned to `resources-task-<controlPlaneId>`
- **Monitoring Contributor** assigned to `diagnostic-settings-task-<controlPlaneId>` and `deployer-task-<controlPlaneId>`
- **Monitoring Contributor** assigned to `initialRunIdentity<controlPlaneId>` (Bicep only)

### Method C: View from Resource Group IAM

This method shows role assignments specific to a resource group.

**Navigation Path:**
```
Azure Portal → Resource Groups → [Resource Group] → Access control (IAM) → Role assignments
```

**Step-by-step:**

1. Navigate to **Resource Groups**
2. Select a **monitored resource group** (where forwarders are deployed)
3. Click **Access control (IAM)** in the left sidebar
4. Click the **Role assignments** tab
5. Look for your managed identities

**What to look for:**

At the resource group level, you should see:
- **Reader and Data Access** assigned to `diagnostic-settings-task-<controlPlaneId>`
- **Contributor** assigned to `scaling-task-<controlPlaneId>` and `deployer-task-<controlPlaneId>`
- **Contributor** assigned to `initialRunIdentity<controlPlaneId>` (Bicep only)

**For the control plane resource group**, you should also see:
- **Website Contributor** assigned to `deployer-task-<controlPlaneId>`
- **ContainerAppStartRole** assigned to `initialRunIdentity<controlPlaneId>` (Bicep only)

### Method D: View from Resource IAM

This method shows role assignments for individual resources (like storage accounts).

**Navigation Path:**
```
Azure Portal → [Resource] → Access control (IAM) → Role assignments
```

**Step-by-step:**

1. Navigate to the specific resource (e.g., control plane storage account)
2. Click **Access control (IAM)** in the left sidebar
3. Click the **Role assignments** tab

**What to look for (Terraform deployments only):**
- **Storage Blob Data Contributor** assigned to `resources-task-<controlPlaneId>` on the control plane storage account

### Understanding Role Assignment Details

When viewing any role assignment, click on it to see:

- **Properties:**
  - Name (GUID)
  - Type (Microsoft.Authorization/roleAssignments)
  - Role definition (the specific role name)
  - Principal ID (the managed identity)
  - Scope (where the permission applies)
  - Description (often includes `ddlfo<controlPlaneId>` for Terraform deployments)

- **Condition:** Some role assignments may have conditions that limit when permissions apply (typically none for this system)

---

## 3. Viewing Custom Roles

Custom roles define specific permissions tailored to your needs. The orchestration creates one custom role (Bicep deployments only).

### View Custom Role Definition

**Navigation Path:**
```
Azure Portal → Subscriptions → [Subscription] → Access control (IAM) → Roles → Type: CustomRole
```

**Step-by-step:**

1. Navigate to your **control plane subscription**
2. Click **Access control (IAM)** in the left sidebar
3. Click the **Roles** tab
4. In the **Type** dropdown filter, select **CustomRole**
5. Look for the role:
   - `ContainerAppStartRole<controlPlaneId>` or `ContainerAppStartRole{controlPlaneId}`

**Viewing role details:**

1. Click on the custom role name
2. On the **Overview** page, you'll see:
   - **Role name**
   - **Description**
   - **Type:** CustomRole
   - **Assignable scopes** (which resource groups can use this role)
3. Click **Permissions** tab to see:
   - **Actions:** `Microsoft.App/jobs/start/action` (allows starting Container App jobs)
   - **NotActions:** (none)
   - **DataActions:** (none)
   - **NotDataActions:** (none)
4. Click **Assignments** tab to see which identities have this role:
   - Should show `initialRunIdentity<controlPlaneId>` assigned at the control plane resource group scope

**Purpose of this role:**
This minimal custom role grants only the permission needed for the initial run identity to trigger the deployer container app job without giving broader permissions.

---

## 4. Troubleshooting Tips

### Issue: "I can't see the Identity tab on my Function App"

**Cause:** You may not have sufficient permissions to view identity information.

**Solution:**
- Verify you have at least **Reader** role on the resource
- Contact your Azure administrator for **Security Reader** or **Reader** permissions

### Issue: "Role assignments are missing or incomplete"

**Cause:** Deployment may have failed partially, or permissions haven't propagated yet.

**Solution:**
1. Wait 5-10 minutes - Azure role assignments can take time to propagate
2. Check deployment logs in the Azure Portal:
   - Go to **Resource Groups** → [Your RG] → **Deployments**
   - Review the deployment status and error messages
3. For Terraform: Run `terraform plan` to check for drift
4. Redeploy if necessary

### Issue: "Functions are failing with authorization errors"

**Cause:** Managed identity may not have the correct role assignments.

**Solution:**
1. Check the Function App logs:
   - Function App → **Log stream** or **Application Insights**
2. Look for error messages like "403 Forbidden" or "AuthorizationFailed"
3. Verify the identity has the expected role using Method A above
4. Common missing roles:
   - **Monitoring Reader** - Required for resources-task to discover resources
   - **Monitoring Contributor** - Required for diagnostic-settings-task to configure diagnostic settings
   - **Reader and Data Access** - Required for diagnostic-settings-task to access storage accounts

### Issue: "Cannot find custom role ContainerAppStartRole"

**Cause:** You may have deployed using Terraform or Standalone templates, which don't create this role.

**Solution:**
- This is expected for Terraform and Standalone deployments
- Only Bicep deployments create the custom ContainerAppStartRole
- Verify your deployment method and check the appropriate documentation

### Issue: "Too many role assignments to review manually"

**Solution:**
Use the **Export** feature:
1. Go to **Access control (IAM)** → **Role assignments**
2. Click **Download role assignments** (Download icon at top)
3. Opens a filtered view - select scope and click **Download**
4. Review in Excel or CSV

**Alternative:** Use Azure CLI or PowerShell scripts (see repository for automation scripts)

### Issue: "Deployment succeeded but no resources are being discovered"

**Cause:** Role assignments may not have propagated, or the identity principal ID is incorrect.

**Solution:**
1. Wait 10-15 minutes for Azure AD propagation
2. Verify the **Principal ID** of the managed identity matches the role assignment:
   - Get Principal ID from the Function App Identity tab
   - Search for this Principal ID in the subscription's role assignments
3. Check that the role assignment scope is correct:
   - Should be at subscription level for Monitoring Reader/Contributor
   - Should be at resource group level for Contributor/Reader and Data Access

### Issue: "How do I know if I'm using Bicep or Terraform deployment?"

**Solution:**
Check your control plane resource group:
- **Bicep deployment:** Will have a User-Assigned Managed Identity named `initialRunIdentity<controlPlaneId>`
- **Terraform deployment:** Will only have system-assigned identities (none in the resource list)
- **Check tags:** Resources may be tagged with deployment method

---

## 5. Quick Reference

### Navigation Paths Table

| Task | Navigation Path |
|------|----------------|
| View Function App identity | Resource Groups → [Control Plane RG] → [Function App] → Identity |
| View Container App identity | Resource Groups → [Control Plane RG] → [Container App Job] → Identity |
| View user-assigned identity | Resource Groups → [Control Plane RG] → [Managed Identity] |
| View identity's role assignments | [Resource] → Identity → Azure role assignments |
| View subscription role assignments | Subscriptions → [Subscription] → Access control (IAM) → Role assignments |
| View resource group role assignments | Resource Groups → [Resource Group] → Access control (IAM) → Role assignments |
| View custom roles | Subscriptions → [Subscription] → Access control (IAM) → Roles → Type: CustomRole |
| Export role assignments | [Scope] → Access control (IAM) → Download role assignments |
| View deployment logs | Resource Groups → [Resource Group] → Deployments |

### Azure Portal Search Shortcuts

Use the search bar at the top of the Azure Portal for quick navigation:

- Search **"resource-group-name identity"** - Jump directly to identities in a resource group
- Search **"resources-task"** - Find your resources-task Function App
- Search **"IAM"** or **"Access control"** - Quick access to IAM blade for current context
- Search **"Role assignments"** - View all role assignments you have access to

### Role Assignment Validation Checklist

Use this checklist to verify your deployment has created all expected permissions:

**Subscription-Level Roles:**
- [ ] Resources Task has **Monitoring Reader** on each monitored subscription
- [ ] Diagnostic Settings Task has **Monitoring Contributor** on each monitored subscription
- [ ] Deployer Task has **Monitoring Contributor** on each monitored subscription
- [ ] Initial Run Identity has **Monitoring Contributor** on each monitored subscription (Bicep only)

**Resource Group-Level Roles:**
- [ ] Diagnostic Settings Task has **Reader and Data Access** on each monitored resource group
- [ ] Scaling Task has **Contributor** on each monitored resource group
- [ ] Deployer Task has **Contributor** on each monitored resource group
- [ ] Initial Run Identity has **Contributor** on each monitored resource group (Bicep only)
- [ ] Deployer Task has **Website Contributor** on control plane resource group
- [ ] Initial Run Identity has **ContainerAppStartRole** on control plane resource group (Bicep only)

**Resource-Level Roles (Terraform only):**
- [ ] Resources Task has **Storage Blob Data Contributor** on control plane storage account

---

## Additional Resources

- **Azure RBAC Documentation:** https://learn.microsoft.com/azure/role-based-access-control/
- **Managed Identities Documentation:** https://learn.microsoft.com/azure/active-directory/managed-identities-azure-resources/
- **Custom Roles Documentation:** https://learn.microsoft.com/azure/role-based-access-control/custom-roles

For automated inspection using Azure CLI, PowerShell, or Terraform state, see the repository documentation.

---

**Last Updated:** January 2025
