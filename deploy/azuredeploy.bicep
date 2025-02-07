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

param datadogTelemetry bool = false
param logLevel string = 'INFO'

param imageRegistry string = 'datadoghq.azurecr.io'
#disable-next-line no-hardcoded-env-urls
param storageAccountUrl string = 'https://ddazurelfo.blob.core.windows.net'

func sub_uuid(uuid string) string => toLower(substring(uuid, 24, 12))

// sub-uuid for the control plane is based on the identifiers below.
// This is to be consistent if there are multiple deploys, while still making a unique id.
// - the management group
// - control plane subscription id
// - control plane resource group name
// - control plane region
var controlPlaneId = sub_uuid(guid(
  managementGroup().id,
  controlPlaneSubscriptionId,
  controlPlaneResourceGroupName,
  controlPlaneLocation
))

module controlPlaneResourceGroup './control_plane_resource_group.bicep' = {
  name: 'controlPlaneResourceGroup-${controlPlaneId}'
  scope: subscription(controlPlaneSubscriptionId)
  params: {
    controlPlaneResourceGroup: controlPlaneResourceGroupName
    controlPlaneLocation: controlPlaneLocation
  }
}

module validateAPIKey './validate_key.bicep' = {
  name: 'validateAPIKey-${controlPlaneId}'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    datadogApiKey: datadogApiKey
    datadogSite: datadogSite
  }
  dependsOn: [
    controlPlaneResourceGroup
  ]
}

module controlPlane './control_plane.bicep' = {
  name: 'controlPlane-${controlPlaneId}'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    controlPlaneId: controlPlaneId
    controlPlaneLocation: controlPlaneLocation
    controlPlaneResourceGroupName: controlPlaneResourceGroupName
    controlPlaneSubscriptionId: controlPlaneSubscriptionId
    datadogApiKey: datadogApiKey
    datadogApplicationKey: datadogApplicationKey
    datadogSite: datadogSite
    datadogTelemetry: datadogTelemetry
    imageRegistry: imageRegistry
    storageAccountUrl: storageAccountUrl
    logLevel: logLevel
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
    name: 'subscriptionPermissions-${sub_uuid(subscriptionId)}-${controlPlaneId}'
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
