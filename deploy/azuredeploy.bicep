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

module controlPlaneSubscription './subscription.bicep' = {
  name: 'createControlPlaneResourceGroup'
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

module controlPlaneResourceGroup './control_plane.bicep' = {
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
  }
  dependsOn: [
    controlPlaneSubscription
  ]
}

var resourceTaskPrincipalId = controlPlaneResourceGroup.outputs.resourceTaskPrincipalId
var diagnosticSettingsTaskPrincipalId = controlPlaneResourceGroup.outputs.diagnosticSettingsTaskPrincipalId
var scalingTaskPrincipalId = controlPlaneResourceGroup.outputs.scalingTaskPrincipalId
var deployerTaskPrincipalId = controlPlaneResourceGroup.outputs.deployerTaskPrincipalId

var contributorRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'b24988ac-6180-42a0-ab88-20f7382dd24c'
)
var monitoringReaderRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '43d0d8ad-25c7-4714-9337-8ba259a9fe05'
)
var monitoringContributorRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
)
var readerAndDataAccessRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'c12c1c16-33a1-487b-954d-41c89c60f349'
)
var websiteContributorRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'de139f84-1756-47ae-9be6-808fbbe84772'
)

resource resourceTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('resourceTask', deployment().name)
  properties: {
    roleDefinitionId: monitoringReaderRole
    principalId: resourceTaskPrincipalId
  }
}

resource diagnosticSettingsTaskMonitorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('monitor', 'diagnosticSettings', deployment().name)
  properties: {
    roleDefinitionId: monitoringContributorRole
    principalId: diagnosticSettingsTaskPrincipalId
  }
}

resource diagnosticSettingsTaskStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('storage', 'diagnosticSettings', deployment().name)
  properties: {
    roleDefinitionId: readerAndDataAccessRole
    principalId: diagnosticSettingsTaskPrincipalId
  }
}

resource scalingTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('scaling', deployment().name)
  properties: {
    roleDefinitionId: contributorRole
    principalId: scalingTaskPrincipalId
  }
}

resource deployerTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('deployer', deployment().name)
  properties: {
    roleDefinitionId: websiteContributorRole
    principalId: deployerTaskPrincipalId
  }
}
