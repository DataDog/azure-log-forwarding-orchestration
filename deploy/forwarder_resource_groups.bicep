targetScope = 'subscription'

param resourceGroupName string
param location string
param controlPlaneId string
param resourceTaskPrincipalId string
param diagnosticSettingsTaskPrincipalId string
param scalingTaskPrincipalId string

resource forwarderResourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: resourceGroupName
  location: location
}

module forwarderResourceGroupPermissions './forwarder_resource_group_permissions.bicep' = {
  name: 'forwarderResourceGroupPermissions'
  scope: forwarderResourceGroup
  params: {
    controlPlaneId: controlPlaneId
    diagnosticSettingsTaskPrincipalId: diagnosticSettingsTaskPrincipalId
    scalingTaskPrincipalId: scalingTaskPrincipalId
  }
}

resource monitoringReaderRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: subscription()
  name: '43d0d8ad-25c7-4714-9337-8ba259a9fe05'
}
resource monitoringContributorRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: subscription()
  name: '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
}

resource resourceTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('resourceTask', controlPlaneId)
  properties: {
    roleDefinitionId: monitoringReaderRole.id
    principalId: resourceTaskPrincipalId
  }
}

resource diagnosticSettingsTaskMonitorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('monitor', 'diagnosticSettings', controlPlaneId)
  properties: {
    roleDefinitionId: monitoringContributorRole.id
    principalId: diagnosticSettingsTaskPrincipalId
  }
}
