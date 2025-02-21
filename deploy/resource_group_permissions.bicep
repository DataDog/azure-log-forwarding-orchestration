targetScope = 'resourceGroup'

param controlPlaneId string
param diagnosticSettingsTaskPrincipalId string
param scalingTaskPrincipalId string
param initialRunPrincipalId string

resource readerAndDataAccessRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: resourceGroup()
  // Details: https://www.azadvertizer.net/azrolesadvertizer/c12c1c16-33a1-487b-954d-41c89c60f349.html
  name: 'c12c1c16-33a1-487b-954d-41c89c60f349'
}

resource contributorRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: resourceGroup()
  // Details: https://www.azadvertizer.net/azrolesadvertizer/b24988ac-6180-42a0-ab88-20f7382dd24c.html
  name: 'b24988ac-6180-42a0-ab88-20f7382dd24c'
}

resource diagnosticSettingsTaskStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, 'storage', 'diagnosticSettings', controlPlaneId)
  scope: resourceGroup()
  properties: {
    description: 'ddlfo${controlPlaneId}'
    roleDefinitionId: readerAndDataAccessRole.id
    principalId: diagnosticSettingsTaskPrincipalId
  }
}

resource scalingTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, 'scaling', controlPlaneId)
  scope: resourceGroup()
  properties: {
    description: 'ddlfo${controlPlaneId}'
    roleDefinitionId: contributorRole.id
    principalId: scalingTaskPrincipalId
  }
}

resource initialRunRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, 'initialRunContributor', controlPlaneId)
  scope: resourceGroup()
  properties: {
    description: 'ddlfo${controlPlaneId}'
    roleDefinitionId: contributorRole.id
    principalId: initialRunPrincipalId
  }
}
