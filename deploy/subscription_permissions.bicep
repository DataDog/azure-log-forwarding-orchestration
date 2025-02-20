targetScope = 'subscription'

param resourceGroupName string
param location string
param controlPlaneId string
param resourceTaskPrincipalId string
param diagnosticSettingsTaskPrincipalId string
param scalingTaskPrincipalId string
param initialRunIdentityPrincipalId string

// create the resource group for the forwarders in this subscription
resource forwarderResourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: resourceGroupName
  location: location
}

// assign the resource group level permissions
module resourceGroupPermissions './resource_group_permissions.bicep' = {
  name: 'resourceGroupPermissions-${controlPlaneId}'
  scope: forwarderResourceGroup
  params: {
    controlPlaneId: controlPlaneId
    diagnosticSettingsTaskPrincipalId: diagnosticSettingsTaskPrincipalId
    scalingTaskPrincipalId: scalingTaskPrincipalId
    initialRunPrincipalId: initialRunIdentityPrincipalId
  }
}

resource monitoringReaderRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: subscription()
  // Details: https://www.azadvertizer.net/azrolesadvertizer/43d0d8ad-25c7-4714-9337-8ba259a9fe05.html
  name: '43d0d8ad-25c7-4714-9337-8ba259a9fe05'
}
resource monitoringContributorRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: subscription()
  // Details: https://www.azadvertizer.net/azrolesadvertizer/749f88d5-cbae-40b8-bcfc-e573ddc772fa.html
  name: '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
}

resource resourceTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, 'resourceTask', controlPlaneId)
  properties: {
    description: 'ddlfo${controlPlaneId}'
    roleDefinitionId: monitoringReaderRole.id
    principalId: resourceTaskPrincipalId
  }
}

resource diagnosticSettingsTaskMonitorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, 'monitor', 'diagnosticSettings', controlPlaneId)
  properties: {
    description: 'ddlfo${controlPlaneId}'
    roleDefinitionId: monitoringContributorRole.id
    principalId: diagnosticSettingsTaskPrincipalId
  }
}


resource initialRunMonitorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, 'initialRunMonitoringContributor', controlPlaneId)
  properties: {
    description: 'ddlfo${controlPlaneId}'
    roleDefinitionId: monitoringContributorRole.id
    principalId: initialRunIdentityPrincipalId
  }
}
