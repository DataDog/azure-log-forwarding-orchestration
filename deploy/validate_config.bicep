targetScope = 'resourceGroup'

@secure()
param datadogApiKey string
param datadogSite string
param piiScrubberRules string

resource validateConfigScript 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'validateConfigScript'
  location: resourceGroup().location
  kind: 'AzureCLI'
  properties: {
    azCliVersion: '2.67.0'
    environmentVariables: [
      { name: 'DD_API_KEY', secureValue: datadogApiKey }
      { name: 'DD_SITE', value: datadogSite }
      { name: 'PII_SCRUBBER_RULES', value: piiScrubberRules }
    ]
    scriptContent: '''
      # Validate Datadog API key with chosen site
      tdnf install -y jq
      response=$(curl -X GET "https://api.${DD_SITE}/api/v1/validate" \
        -H "Accept: application/json" \
        -H "DD-API-KEY: ${DD_API_KEY}" 2>/dev/null)
      if [ "$(jq .valid <<<"$response")" != 'true' ]; then
        echo "{\"Result\": {\"error\": \"Unable to validate API Key against Site '${DD_SITE}'. Please check that the correct Datadog host site was used and that the key is a valid Datadog API key found at https://app.datadoghq.com/organization-settings/api-keys\", \"response\": $response}}" | jq | tee "$AZ_SCRIPTS_OUTPUT_PATH"
        exit 1
      fi

      if [[ -z "$PII_SCRUBBER_RULES" ]]; then
        exit 0
      fi
      
      # Validate PII rules are parseable YAML 
      curl -sSLo /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 && chmod +x /usr/local/bin/yq
      yq_output=$(echo "$PII_SCRUBBER_RULES" | yq '.' 2>&1)
      if [ $? -ne 0 ]; then
        echo "{\"Result\": {\"error\": \"Invalid YAML. Please provide a valid YAML file.\", \"details\": \"$yq_output\"}}" | jq | tee "$AZ_SCRIPTS_OUTPUT_PATH"
        exit 1
      fi
    '''
    timeout: 'PT30M'
    cleanupPreference: 'OnSuccess'
    retentionInterval: 'PT1H'
  }
}
