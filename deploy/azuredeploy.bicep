targetScope = 'managementGroup'

param monitoredSubscriptions string

param controlPlaneLocation string
param controlPlaneSubscriptionId string
param controlPlaneResourceGroupName string

@secure()
@description('Datadog API Key')
param datadogApiKey string
@description('Datadog Site')
param datadogSite string
@description('Comma separated list of tags to filter resources by')
param resourceTagFilters string = ''
@description('YAML formatted list of PII Scrubber Rules')
param piiScrubberRules string = ''
param datadogTelemetry bool = false
param logLevel string = 'INFO'

param imageRegistry string = 'datadoghq.azurecr.io'
#disable-next-line no-hardcoded-env-urls
param storageAccountUrl string = 'https://ddazurelfo.blob.core.windows.net'

func subUuid(uuid string) string => toLower(substring(uuid, 24, 12))

// sub-uuid for the control plane is based on the identifiers below.
// This is to be consistent if there are multiple deploys, while still making a unique id.
// - the management group
// - control plane subscription id
// - control plane resource group name
// - control plane region
var controlPlaneId = subUuid(guid(
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

module validateConfig './validate_config.bicep' = {
  name: 'validateConfig-${controlPlaneId}'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    datadogApiKey: datadogApiKey
    datadogSite: datadogSite
    piiScrubberRules: piiScrubberRules
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
    monitoredSubscriptions: monitoredSubscriptions
    datadogApiKey: datadogApiKey
    datadogSite: datadogSite
    datadogTelemetry: datadogTelemetry
    resourceTagFilters: resourceTagFilters
    piiScrubberRules: piiScrubberRules
    imageRegistry: imageRegistry
    storageAccountUrl: storageAccountUrl
    logLevel: logLevel
  }
  dependsOn: [
    controlPlaneResourceGroup
    validateConfig
  ]
}

var resourceTaskPrincipalId = controlPlane.outputs.resourceTaskPrincipalId
var diagnosticSettingsTaskPrincipalId = controlPlane.outputs.diagnosticSettingsTaskPrincipalId
var scalingTaskPrincipalId = controlPlane.outputs.scalingTaskPrincipalId

// create the subscription level permissions, as well as the resource group for forwarders and the permissions on that resource group
module subscriptionPermissions './subscription_permissions.bicep' = [
  for subscriptionId in json(monitoredSubscriptions): {
    name: 'subscriptionPermissions-${subUuid(subscriptionId)}-${controlPlaneId}'
    scope: subscription(subscriptionId)
    params: {
      resourceGroupName: controlPlaneResourceGroupName
      location: controlPlaneLocation
      controlPlaneId: controlPlaneId
      resourceTaskPrincipalId: resourceTaskPrincipalId
      diagnosticSettingsTaskPrincipalId: diagnosticSettingsTaskPrincipalId
      scalingTaskPrincipalId: scalingTaskPrincipalId
      initialRunIdentityPrincipalId: controlPlane.outputs.initialRunIdentityPrincipalId
    }
  }
]

module initialRun './initial_run.bicep' = {
  name: 'initialRun-${controlPlaneId}'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    controlPlaneId: controlPlaneId
    initialRunIdentity: controlPlane.outputs.initialRunIdentityId
    storageAccountName: controlPlane.outputs.storageAccountName
    datadogApiKey: datadogApiKey
    datadogSite: datadogSite
    datadogTelemetry: datadogTelemetry
    logLevel: logLevel
    monitoredSubscriptions: monitoredSubscriptions
    piiScrubberRules: piiScrubberRules
    resourceTagFilters: resourceTagFilters
    forwarderImage: '${imageRegistry}/forwarder:latest'
    storageAccountUrl: storageAccountUrl
  }
  dependsOn: [
    subscriptionPermissions
  ]
}
