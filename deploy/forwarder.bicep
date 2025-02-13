// This will deploy just the forwarder container app + storage account needed for the forwarder to work.
// Automated log forwarding setup and scaling will not be set up.
targetScope = 'resourceGroup'


@description('Name of the Container App Managed Environment for the Forwarder')
param environmentName string = 'datadog-log-forwarder-env'

@description('Name of the Forwarder Container App Job')
param jobName string = 'datadog-log-forwarder'

@description('Name of the Log Storage Account')
param storageAccountName string = 'datadoglogstorage'

@description('The SKU of the storage account')
param storageAccountSku
  | 'Premium_LRS'
  | 'Premium_ZRS'
  | 'Standard_GRS'
  | 'Standard_GZRS'
  | 'Standard_LRS'
  | 'Standard_RAGRS'
  | 'Standard_RAGZRS'
  | 'Standard_ZRS' = 'Standard_LRS'

@secure()
param datadogApiKey string

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: resourceGroup().location
  sku: {
    name: storageAccountSku
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource forwarderEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: resourceGroup().location
  properties: {}
}

resource forwarder 'Microsoft.App/jobs@2023-05-01' = {
  name: jobName
  location: resourceGroup().location
  properties: {
    environmentId: forwarderEnvironment.id
    configuration: {
      triggerType: 'Schedule'
      replicaTimeout: 1800
      replicaRetryLimit: 1
      scheduleTriggerConfig: {
        cronExpression: '* * * * *'
        parallelism: 1
        replicaCompletionCount: 1
      }
      secrets: [
        {
          name: 'storage-connection-string'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};EndpointSuffix=${environment().suffixes.storage};AccountKey=${storageAccount.listKeys().keys[0].value}'
        }
        { name: 'dd-api-key', value: datadogApiKey }
      ]
    }
    template: {
      containers: [
        {
          name: 'datadog-forwarder'
          image: 'datadoghq.azurecr.io/forwarder:latest'
          resources: {
            cpu: 1
            memory: '2Gi'
          }
          env: [
            { name: 'AzureWebJobsStorage', secretRef: 'storage-connection-string' }
            { name: 'DD_API_KEY', secretRef: 'dd-api-key' }
          ]
        }
      ]
    }
  }
}
