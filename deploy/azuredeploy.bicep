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
  name: 'controlPlaneResourceGroup'
  scope: subscription(controlPlaneSubscriptionId)
  params: {
    controlPlaneResourceGroup: controlPlaneResourceGroupName
    controlPlaneLocation: controlPlaneLocation
  }
}

module validateAPIKey './validate_key.bicep' = {
  name: 'validateAPIKey'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    datadogApiKey: datadogApiKey
    datadogSite: datadogSite
  }
  dependsOn: [
    controlPlaneResourceGroup
  ]
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
  name: 'controlPlane'
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
    validateAPIKey
  ]
}

var resourceTaskPrincipalId = controlPlane.outputs.resourceTaskPrincipalId
var diagnosticSettingsTaskPrincipalId = controlPlane.outputs.diagnosticSettingsTaskPrincipalId
var scalingTaskPrincipalId = controlPlane.outputs.scalingTaskPrincipalId

// create the subscription level permissions, as well as the resource group for forwarders and the permissions on that resource group
module subscriptionPermissions './subscription_permissions.bicep' = [
  for subscriptionId in json(monitoredSubscriptions): {
    name: 'subscriptionPermissions-${subscriptionId}'
    scope: subscription(subscriptionId)
    params: {
      resourceGroupName: controlPlaneResourceGroupName
      location: controlPlaneLocation
      controlPlaneId: controlPlaneId
      resourceTaskPrincipalId: resourceTaskPrincipalId
      diagnosticSettingsTaskPrincipalId: diagnosticSettingsTaskPrincipalId
      scalingTaskPrincipalId: scalingTaskPrincipalId
    }
  }
]
