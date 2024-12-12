targetScope = 'resourceGroup'

param controlPlaneId string

param controlPlaneLocation string
param controlPlaneSubscriptionId string
param controlPlaneResourceGroupName string

param imageRegistry string
param storageAccountUrl string

@description('Datadog API Key')
@secure()
param datadogApiKey string

@description('Datadog App Key')
@secure()
param datadogApplicationKey string

@description('Datadog Site')
param datadogSite string

var deployerTaskImage = '${imageRegistry}/deployer:latest'
var forwarderImage = '${imageRegistry}/forwarder:latest'

// CONTROL PLANE RESOURCES

resource asp 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: 'control-plane-asp-${controlPlaneId}'
  location: controlPlaneLocation
  kind: 'linux'
  properties: {
    reserved: true
  }
  sku: {
    tier: 'Dynamic'
    name: 'Y1'
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: 'lfostorage${controlPlaneId}'
  kind: 'StorageV2'
  location: controlPlaneLocation
  properties: { accessTier: 'Hot' }
  sku: { name: 'Standard_LRS' }
}

resource fileServices 'Microsoft.Storage/storageAccounts/fileServices@2023-05-01' = {
  name: 'default'
  parent: storageAccount
  properties: {}
}

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  name: 'default'
  parent: storageAccount
  properties: {}
}

resource cacheContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: 'control-plane-cache'
  parent: blobServices
  properties: {}
}

var storageAccountKey = listKeys(storageAccount.id, '2019-06-01').keys[0].value
var connectionString = 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${storageAccountKey}'

var commonAppSettings = [
  { name: 'AzureWebJobsStorage', value: connectionString }
  { name: 'AzureWebJobsFeatureFlags', value: 'EnableWorkerIndexing' }
  { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
  { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
  { name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING', value: connectionString }
]

var resourceTaskName = 'resources-task-${controlPlaneId}'
resource resourceTask 'Microsoft.Web/sites@2022-09-01' = {
  name: resourceTaskName
  location: controlPlaneLocation
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: asp.id
    siteConfig: {
      appSettings: union(commonAppSettings, [
        { name: 'WEBSITE_CONTENTSHARE', value: resourceTaskName }
      ])
      linuxFxVersion: 'Python|3.11'
    }
    publicNetworkAccess: 'Enabled'
    httpsOnly: true
  }
  dependsOn: [fileServices]
}
var diagnosticSettingsTaskName = 'diagnostic-settings-task-${controlPlaneId}'
resource diagnosticSettingsTask 'Microsoft.Web/sites@2022-09-01' = {
  name: diagnosticSettingsTaskName
  location: controlPlaneLocation
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: asp.id
    siteConfig: {
      appSettings: union(commonAppSettings, [
        { name: 'RESOURCE_GROUP', value: controlPlaneResourceGroupName }
        { name: 'WEBSITE_CONTENTSHARE', value: resourceTaskName }
        { name: 'CONTROL_PLANE_ID', value: controlPlaneId }
      ])
      linuxFxVersion: 'Python|3.11'
    }
    publicNetworkAccess: 'Enabled'
    httpsOnly: true
  }
  dependsOn: [fileServices]
}

var scalingTaskName = 'scaling-task-${controlPlaneId}'
resource scalingTask 'Microsoft.Web/sites@2022-09-01' = {
  name: scalingTaskName
  location: controlPlaneLocation
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: asp.id
    siteConfig: {
      appSettings: union(commonAppSettings, [
        { name: 'RESOURCE_GROUP', value: controlPlaneResourceGroupName }
        { name: 'WEBSITE_CONTENTSHARE', value: resourceTaskName }
        { name: 'FORWARDER_IMAGE', value: forwarderImage }
        { name: 'DD_API_KEY', value: datadogApiKey }
        { name: 'DD_APP_KEY', value: datadogApplicationKey }
        { name: 'DD_SITE', value: datadogSite }
        { name: 'CONTROL_PLANE_REGION', value: controlPlaneLocation }
        { name: 'CONTROL_PLANE_ID', value: controlPlaneId }
      ])
      linuxFxVersion: 'Python|3.11'
    }
    publicNetworkAccess: 'Enabled'
    httpsOnly: true
  }
  dependsOn: [fileServices]
}

resource deployerTaskEnv 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: 'deployer-task-env-${controlPlaneId}'
  location: controlPlaneLocation
  properties: {}
}

var deployerTaskName = 'deployer-task-${controlPlaneId}'

resource deployerTask 'Microsoft.App/jobs@2024-03-01' = {
  name: deployerTaskName
  location: controlPlaneLocation
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    environmentId: deployerTaskEnv.id
    configuration: {
      triggerType: 'Schedule'
      scheduleTriggerConfig: {
        cronExpression: '*/30 * * * *'
      }
      replicaRetryLimit: 1
      replicaTimeout: 1800
      secrets: [
        { name: 'connection-string', value: connectionString }
        { name: 'dd-api-key', value: datadogApiKey }
        { name: 'dd-app-key', value: datadogApplicationKey }
      ]
    }
    template: {
      containers: [
        {
          name: deployerTaskName
          image: deployerTaskImage
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            { name: 'AzureWebJobsStorage', secretRef: 'connection-string' }
            { name: 'SUBSCRIPTION_ID', value: controlPlaneSubscriptionId }
            { name: 'RESOURCE_GROUP', value: controlPlaneResourceGroupName }
            { name: 'REGION', value: controlPlaneLocation }
            { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
            { name: 'DD_APP_KEY', secretRef: 'dd-app-key' }
            { name: 'DD_SITE', value: datadogSite }
            { name: 'STORAGE_ACCOUNT_URL', value: storageAccountUrl }
          ]
        }
      ]
    }
  }
}

resource websiteContributorRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: resourceGroup()
  // Details: https://www.azadvertizer.net/azrolesadvertizer/de139f84-1756-47ae-9be6-808fbbe84772.html
  name: 'de139f84-1756-47ae-9be6-808fbbe84772'
}

resource deployerTaskRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('deployer', controlPlaneId)
  scope: resourceGroup()
  properties: {
    roleDefinitionId: websiteContributorRole.id
    principalId: deployerTask.identity.principalId
  }
}

output resourceTaskPrincipalId string = resourceTask.identity.principalId
output diagnosticSettingsTaskPrincipalId string = diagnosticSettingsTask.identity.principalId
output scalingTaskPrincipalId string = scalingTask.identity.principalId

// DEPLOYER TASK INITIAL RUN

resource runInitialDeployIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2018-11-30' = {
  name: 'runInitialDeployIdentity'
  location: controlPlaneLocation
}

resource containerAppsContributorRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' existing = {
  scope: resourceGroup()
  // Details: https://www.azadvertizer.net/azrolesadvertizer/358470bc-b998-42bd-ab17-a7e34c199c0f.html
  name: '358470bc-b998-42bd-ab17-a7e34c199c0f'
}

resource runInitialDeployIdentityRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid('runInitialDeployIdentityRoleAssignment', controlPlaneId)
  properties: {
    roleDefinitionId: containerAppsContributorRole.id
    principalId: runInitialDeployIdentity.properties.principalId
  }
}

resource runInitialDeploy 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'runInitialDeploy'
  location: controlPlaneLocation
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${runInitialDeployIdentity.id}': {} }
  }
  properties: {
    storageAccountSettings: {
      // reuse the storage account from before
      storageAccountName: storageAccount.name
      storageAccountKey: storageAccountKey
    }
    azCliVersion: '2.64.0'
    scriptContent: 'set -e; az extension add --name containerapp --allow-preview true; az containerapp job start --name ${deployerTaskName} --resource-group ${controlPlaneResourceGroupName}'
    timeout: 'PT30M'
    retentionInterval: 'PT1H'
    cleanupPreference: 'OnSuccess'
  }
  dependsOn: [
    runInitialDeployIdentityRoleAssignment
    deployerTaskRole
  ]
}
