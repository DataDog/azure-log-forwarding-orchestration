targetScope = 'resourceGroup'

@secure()
param datadogApiKey string
param datadogSite string

resource httpRequest 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'httpRequestScript'
  location: resourceGroup().location
  kind: 'AzureCLI'
  properties: {
    azCliVersion: '2.0.80'
    environmentVariables: [
      { name: 'DD_API_KEY', secureValue: datadogApiKey }
      { name: 'DD_SITE', value: datadogSite }
    ]
    scriptContent: '''
      response=$(curl -X GET "https://api.${DD_SITE}/api/v1/validate" \
        -H "Accept: application/json" \
        -H "DD-API-KEY: ${DD_API_KEY}" 2>/dev/null)
      if [ "$response" != '{"valid":true}' ]; then
        echo "Unable to validate API Key against Site '${DD_SITE}': $response"
        echo Please check your API Key and Site and try again.
        exit 1
      fi
    '''
    timeout: 'PT30M'
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'P0M'
  }
}
