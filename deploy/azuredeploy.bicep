targetScope = 'managementGroup'

param controlPlaneLocation string
param controlPlaneSubscriptionId string
param controlPlaneResourceGroupName string

@secure()
param datadogApplicationKey string
@secure()
param datadogApiKey string
param datadogSite string

module controlPlaneSubscription './subscription.bicep' = {
  name: 'createControlPlaneResourceGroup'
  scope: subscription(controlPlaneSubscriptionId)
  params: {
    controlPlaneResourceGroup: controlPlaneResourceGroupName
    controlPlaneLocation: controlPlaneLocation
  }
}

module controlPlaneResourceGroup './control_plane.bicep' = {
  name: 'controlPlaneResourceGroup'
  scope: resourceGroup(controlPlaneSubscriptionId, controlPlaneResourceGroupName)
  params: {
    controlPlaneLocation: controlPlaneLocation
    controlPlaneResourceGroupName: controlPlaneResourceGroupName
    controlPlaneSubscriptionId: controlPlaneSubscriptionId
    datadogApiKey: datadogApiKey
    datadogApplicationKey: datadogApplicationKey
    datadogSite: datadogSite
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
  '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
)
var monitoringReaderRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
)
var monitoringContributorRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '749f88d5-cbae-40b8-bcfc-e573ddc772fa'
)
var readerAndDataAccessRole = managementGroupResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'c12c1c16-33a1-487b-954d-41c89c60f349'
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

resource scalingTaskId_name 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('scaling', deployment().name)
  properties: {
    roleDefinitionId: contributorRole
    principalId: scalingTaskPrincipalId
  }
}

resource deployerTaskId_name 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('deployer', deployment().name)
  properties: {
    roleDefinitionId: contributorRole
    principalId: deployerTaskPrincipalId
  }
}
