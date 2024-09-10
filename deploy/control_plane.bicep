targetScope = 'resourceGroup'

param controlPlaneLocation string
param controlPlaneSubscriptionId string
param controlPlaneResourceGroupName string

@description('Datadog API Key')
@secure()
param datadogApiKey string

@description('Datadog App Key')
@secure()
param datadogApplicationKey string

@description('Datadog Site')
param datadogSite string

var lfoId = toLower(substring(guid('lfo', deployment().name), 24, 12))

var deployerTaskImage = 'mattlogger.azurecr.io/deployer:latest'
var forwarderImage = 'mattlogger.azurecr.io/forwarder:latest'

resource asp 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: 'control-plane-asp-${lfoId}'
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
  name: 'lfostorage${lfoId}'
  kind: 'BlobStorage'
  location: controlPlaneLocation
  properties: { accessTier: 'Hot' }
  sku: { name: 'Standard_LRS' }
}

resource resourceTask 'Microsoft.Web/sites@2022-09-01' = {
  name: 'resources-task-${lfoId}'
  location: controlPlaneLocation
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: asp.id
    siteConfig: {
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${listKeys(storageAccount.id,'2019-06-01').keys[0].value}'
        }
        { name: 'AzureWebJobsFeatureFlags', value: 'EnableWorkerIndexing' }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
      ]
      linuxFxVersion: 'Python|3.11'
    }
    publicNetworkAccess: 'Disabled'
    httpsOnly: true
  }
}

resource diagnosticSettingsTask 'Microsoft.Web/sites@2022-09-01' = {
  name: 'diagnostic-settings-task-${lfoId}'
  location: controlPlaneLocation
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: asp.id
    siteConfig: {
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${listKeys(storageAccount.id,'2019-06-01').keys[0].value}'
        }
        { name: 'AzureWebJobsFeatureFlags', value: 'EnableWorkerIndexing' }
        { name: 'RESOURCE_GROUP', value: controlPlaneResourceGroupName }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
      ]
      linuxFxVersion: 'Python|3.11'
    }
    publicNetworkAccess: 'Disabled'
    httpsOnly: true
  }
}

resource scalingTask 'Microsoft.Web/sites@2022-09-01' = {
  name: 'scaling-task-${lfoId}'
  location: controlPlaneLocation
  kind: 'functionapp'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: asp.id
    siteConfig: {
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${listKeys(storageAccount.id,'2019-06-01').keys[0].value}'
        }
        { name: 'AzureWebJobsFeatureFlags', value: 'EnableWorkerIndexing' }
        { name: 'RESOURCE_GROUP', value: controlPlaneResourceGroupName }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }
        { name: 'forwarder_image', value: forwarderImage }
        { name: 'DD_API_KEY', value: datadogApiKey }
        { name: 'DD_APP_KEY', value: datadogApplicationKey }
        { name: 'DD_SITE', value: datadogSite }
      ]
      linuxFxVersion: 'Python|3.11'
    }
    publicNetworkAccess: 'Disabled'
    httpsOnly: true
  }
}

resource deployerTaskEnv 'Microsoft.App/managedEnvironments@2022-03-01' = {
  name: 'deployer-task-env-${lfoId}'
  location: controlPlaneLocation
  properties: {}
}

var deployerTaskName = 'deployer-task-${lfoId}'

resource deployerTask 'Microsoft.App/containerApps@2022-03-01' = {
  name: deployerTaskName
  location: controlPlaneLocation
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: deployerTaskEnv.id
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
            {
              name: 'AzureWebJobsStorage'
              value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${listKeys(storageAccount.id,'2019-06-01').keys[0].value}'
            }
            { name: 'SUBSCRIPTION_ID', value: controlPlaneSubscriptionId }
            { name: 'RESOURCE_GROUP', value: controlPlaneResourceGroupName }
            { name: 'REGION', value: controlPlaneLocation }
            { name: 'DD_API_KEY', value: datadogApiKey }
            { name: 'DD_APP_KEY', value: datadogApplicationKey }
            { name: 'DD_SITE', value: datadogSite }
          ]
        }
      ]
    }
  }
}

output resourceTaskPrincipalId string = resourceTask.identity.principalId
output diagnosticSettingsTaskPrincipalId string = diagnosticSettingsTask.identity.principalId
output scalingTaskPrincipalId string = scalingTask.identity.principalId
output deployerTaskPrincipalId string = deployerTask.identity.principalId
