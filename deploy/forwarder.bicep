// This will deploy just the forwarder container app + storage account needed for the forwarder to work.
// Automated log forwarding setup and scaling will not be set up.
targetScope = 'resourceGroup'

@description('Name of the Container App Managed Environment for the Forwarder')
@minLength(2)
@maxLength(60)
param environmentName string = 'datadog-log-forwarder-env'

@description('Name of the Forwarder Container App Job')
@minLength(1)
@maxLength(260)
param jobName string = 'datadog-log-forwarder'

@description('Name of the Log Storage Account')
@minLength(3)
@maxLength(24)
param storageAccountName string = 'ddlog${uniqueString(resourceGroup().id)}'

@description('The SKU of the storage account')
@allowed([
  'Premium_LRS'
  'Premium_ZRS'
  'Standard_GRS'
  'Standard_GZRS'
  'Standard_LRS'
  'Standard_RAGRS'
  'Standard_RAGZRS'
  'Standard_ZRS'
])
param storageAccountSku string = 'Standard_LRS'

@description('The number of days to retain logs (and internal metrics) in the storage account')
@minValue(1)
param storageAccountRetentionDays int = 1

@secure()
@description('Datadog API Key')
@minLength(32)
@maxLength(32)
param datadogApiKey string

@allowed([
  'datadoghq.com'
  'datadoghq.eu'
  'ap1.datadoghq.com'
  'ap2.datadoghq.com'
  'us3.datadoghq.com'
  'us5.datadoghq.com'
  'ddog-gov.com'
])
param datadogSite string = 'datadoghq.com'

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

resource storageManagementPolicy 'Microsoft.Storage/storageAccounts/managementPolicies@2023-05-01' = {
  name: 'default'
  parent: storageAccount
  properties: {
    policy: {
      rules: [
        {
          enabled: true
          name: 'Delete Old Blobs'
          type: 'Lifecycle'
          definition: {
            actions: {
              baseBlob: {
                delete: {
                  daysAfterModificationGreaterThan: storageAccountRetentionDays
                }
              }
              snapshot: {
                delete: {
                  daysAfterCreationGreaterThan: storageAccountRetentionDays
                }
              }
            }
            filters: {
              blobTypes: [
                'blockBlob'
                'appendBlob'
              ]
            }
          }
        }
      ]
    }
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
            { name: 'DD_SITE', value: datadogSite }
            { name: 'CONTROL_PLANE_ID', value: 'none' }
            { name: 'CONFIG_ID', value: resourceId('Microsoft.App/jobs', jobName) }
          ]
        }
      ]
    }
  }
}
