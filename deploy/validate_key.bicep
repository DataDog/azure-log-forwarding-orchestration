targetScope = 'resourceGroup'

@secure()
param datadogApiKey string
param datadogSite string

resource validateAPIKeyScript 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'validateAPIKeyScript'
  location: resourceGroup().location
  kind: 'AzureCLI'
  properties: {
    azCliVersion: '2.64.0'
    environmentVariables: [
      { name: 'DD_API_KEY', secureValue: datadogApiKey }
      { name: 'DD_SITE', value: datadogSite }
    ]
    scriptContent: '''
      response=$(curl -X GET "https://api.${DD_SITE}/api/v1/validate" \
        -H "Accept: application/json" \
        -H "DD-API-KEY: ${DD_API_KEY}" 2>/dev/null)
      if [ "$(jq .valid <<<"$response")" != 'true' ]; then
        echo "{\"Result\": {\"error\": \"Unable to validate API Key against Site '${DD_SITE}'. Please check that the correct Datadog host site was used and that the key is a valid Datadog API key found at https://app.datadoghq.com/organization-settings/api-keys\", \"response\": $response}}" | jq >"$AZ_SCRIPTS_OUTPUT_PATH"
        exit 1
      fi
    '''
    timeout: 'PT30M'
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'PT1H'
  }
}
