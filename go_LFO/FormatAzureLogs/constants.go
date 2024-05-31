package FormatAzureLogs

import (
	"encoding/json"
	"os"
)

const VERSION = "1.0.1"

type AzureLogs struct {
	ByteSize      int
	ForwarderName string
	DDRequire     DDLogs
	Rest          json.RawMessage `json:"-"`
}

type DDLogs struct {
	ResourceId       string `json:"resourceId"` // important
	Category         string `json:"category"`
	Ddsource         string `json:"ddsource"`
	Ddsourcecategory string `json:"ddsourcecategory"`
	Service          string `json:"service"`
	Ddtags           string `json:"ddtags"` // string array of tags
}

// To scrub PII from your logs, uncomment the applicable configs below. If you'd like to scrub more than just
// emails and IP addresses, add your own config to this map in the format
//
//	"REDACT_IP": {
//	    Pattern:     regexp.MustCompile(`[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}`),
//	    Replacement: "xxx.xxx.xxx.xxx",
//	},
//
//	"REDACT_EMAIL": {
//	    Pattern:     regexp.MustCompile(`[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+`),
//	    Replacement: "xxxxx@xxxxx.com",
//	}
type scrubberConfig struct {
	Pattern     string
	Replacement string
}

type ScrubberRuleConfigs map[string]scrubberConfig

// To split array-type fields in your logs into individual logs, you can add sections to the map below. An example of
// a potential use case with azure.datafactory is there to show the format:
//
//	{
//	  source_type:
//	   paths: [list of [list of fields in the log payload to iterate through to find the one to split]],
//	   keep_original_log: bool, if you'd like to preserve the original log in addition to the split ones or not,
//	   preserve_fields: bool, whether to keep the original log fields in the new split logs
//	}
//
// You can also set the DD_LOG_SPLITTING_CONFIG env var with a JSON string in this format.
//
//	"azure.datafactory": {
//	    Paths:           [][]string{{"properties", "Output", "value"}},
//	    KeepOriginalLog: true,
//	    PreserveFields:  true,
//	}
type logSplitConfig struct {
	Paths           []string
	KeepOriginalLog bool
	PreserveFields  bool
}

type AzureLogSplittingConfig map[string]logSplitConfig

var (
	DdApiKey         = getEnvOrDefault("DD_API_KEY", "<DATADOG_API_KEY>")
	DdSite           = getEnvOrDefault("DD_SITE", "datad0g.com")
	DdHttpUrl        = getEnvOrDefault("DD_URL", "http-intake.logs."+DdSite)
	DdHttpPort       = getEnvOrDefault("DD_PORT", "443")
	DdTags           = getEnvOrDefault("DD_TAGS", "") // TODO: Replace '' by your comma-separated list of tags
	DdService        = getEnvOrDefault("DD_SERVICE", "azure")
	DdSource         = getEnvOrDefault("DD_SOURCE", "azure")
	DdSourceCategory = getEnvOrDefault("DD_SOURCE_CATEGORY", "azure")
)

func getEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getLogSplittingConfig() AzureLogSplittingConfig {
	var configMap AzureLogSplittingConfig
	value := os.Getenv("DD_LOG_SPLITTING_CONFIG")
	if value == "" {
		return nil
	}

	err := json.Unmarshal([]byte(value), &configMap)
	if err != nil {
		return nil
	}
	return configMap
}
