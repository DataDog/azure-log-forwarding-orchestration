targetScope = 'managementGroup'

param monitoredSubscriptions string

param controlPlaneLocation string
param controlPlaneSubscriptionId string
param controlPlaneResourceGroupName string

@secure()
param datadogApplicationKey string
@secure()
param datadogApiKey string
param datadogSite string

param imageRegistry string = 'datadoghq.azurecr.io'
#disable-next-line no-hardcoded-env-urls
param storageAccountUrl string = 'https://ddazurelfo.blob.core.windows.net'

module controlPlaneResourceGroup './control_plane_resource_group.bicep' = {
  name: controlPlaneResourceGroupName
  scope: subscription(controlPlaneSubscriptionId)
  params: {
    controlPlaneResourceGroup: controlPlaneResourceGroupName
    controlPlaneLocation: controlPlaneLocation
  }
}

// sub-uuid for the control plane is based on the identifiers below.
// This is to be consistent if there are multiple deploys, while still making a unique id.
// - the management group
// - control plane subscription id
// - control plane resource group name
// - control plane region
var controlPlaneId = toLower(substring(
  guid(managementGroup().id, controlPlaneSubscriptionId, controlPlaneResourceGroupName, controlPlaneLocation),
  24,
  12
))

module controlPlane './control_plane.bicep' = {
  name: 'controlPlaneResourceGroup'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    controlPlaneId: controlPlaneId
    controlPlaneLocation: controlPlaneLocation
    controlPlaneResourceGroupName: controlPlaneResourceGroupName
    controlPlaneSubscriptionId: controlPlaneSubscriptionId
    monitoredSubscriptions: monitoredSubscriptions
    datadogApiKey: datadogApiKey
    datadogApplicationKey: datadogApplicationKey
    datadogSite: datadogSite
    imageRegistry: imageRegistry
    storageAccountUrl: storageAccountUrl
  }
  dependsOn: [
    controlPlaneResourceGroup
  ]
}

var resourceTaskPrincipalId = controlPlane.outputs.resourceTaskPrincipalId
var diagnosticSettingsTaskPrincipalId = controlPlane.outputs.diagnosticSettingsTaskPrincipalId
var scalingTaskPrincipalId = controlPlane.outputs.scalingTaskPrincipalId

var monitoringReaderRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '43d0d8ad-25c7-4714-9337-8ba259a9fe05'
)
var monitoringContributorRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
)

resource resourceTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('resourceTask', controlPlaneId)
  properties: {
    roleDefinitionId: monitoringReaderRole
    principalId: resourceTaskPrincipalId
  }
}

resource diagnosticSettingsTaskMonitorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('monitor', 'diagnosticSettings', controlPlaneId)
  properties: {
    roleDefinitionId: monitoringContributorRole
    principalId: diagnosticSettingsTaskPrincipalId
  }
}

module forwarderResourceGroups './forwarder_resource_groups.bicep' = [
  for subscriptionId in json(monitoredSubscriptions): {
    name: 'forwarderResourceGroups'
    scope: subscription(subscriptionId)
    params: {
      resourceGroupName: 'datadog-forwarder'
      location: controlPlaneLocation
      controlPlaneId: controlPlaneId
      diagnosticSettingsTaskPrincipalId: diagnosticSettingsTaskPrincipalId
      scalingTaskPrincipalId: scalingTaskPrincipalId
    }
  }
]
