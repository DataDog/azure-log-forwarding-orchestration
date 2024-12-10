targetScope = 'subscription'

param resourceGroupName string
param location string
param controlPlaneId string
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
