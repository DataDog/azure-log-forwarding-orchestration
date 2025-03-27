param initialRunIdentity string
param controlPlaneId string
param storageAccountName string
@secure()
param datadogApiKey string
param datadogSite string
param datadogTelemetry bool
param logLevel string
param monitoredSubscriptions string
param forwarderImage string
param piiScrubberRules string
param resourceTagFilters string
param storageAccountUrl string

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}
var storageAccountKey = listKeys(storageAccount.id, '2019-06-01').keys[0].value
var connectionString = 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};EndpointSuffix=${environment().suffixes.storage};AccountKey=${storageAccountKey}'

resource initialRun 'Microsoft.Resources/deploymentScripts@2023-08-01' = {
  name: 'initialRun'
  location: resourceGroup().location
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${initialRunIdentity}': {} }
  }
  properties: {
    storageAccountSettings: {
      // reuse the storage account from before
      storageAccountName: storageAccount.name
      storageAccountKey: storageAccountKey
    }
    environmentVariables: [
      { name: 'AzureWebJobsStorage', secureValue: connectionString }
      { name: 'DD_API_KEY', secureValue: datadogApiKey }
      { name: 'DD_SITE', value: datadogSite }
      { name: 'DD_TELEMETRY', value: datadogTelemetry ? 'true' : 'false' }
      { name: 'CONTROL_PLANE_ID', value: controlPlaneId }
      { name: 'FORWARDER_IMAGE', value: forwarderImage }
      { name: 'CONTROL_PLANE_REGION', value: resourceGroup().location }
      { name: 'RESOURCE_GROUP', value: resourceGroup().name }
      { name: 'SUBSCRIPTION_ID', value: subscription().subscriptionId }
      { name: 'LOG_LEVEL', value: logLevel }
      { name: 'MONITORED_SUBSCRIPTIONS', value: monitoredSubscriptions }
      { name: 'PII_SCRUBBER_RULES', value: piiScrubberRules }
      { name: 'RESOURCE_TAG_FILTERS', value: resourceTagFilters }
    ]
    azCliVersion: '2.67.0'
    primaryScriptUri: '${storageAccountUrl}/lfo/initial_run.sh'
    timeout: 'PT30M'
    retentionInterval: 'PT1H'
    cleanupPreference: 'OnSuccess'
  }
}
