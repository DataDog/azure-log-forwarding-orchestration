package environment

import (
	"os"
)

// Environment variable names
const (
	APM_ENABLED            = "DD_APM_ENABLED"
	AZURE_WEB_JOBS_STORAGE = "AzureWebJobsStorage"
	CONTROL_PLANE_ID       = "CONTROL_PLANE_ID"
	CONFIG_ID              = "CONFIG_ID"
	DD_API_KEY             = "DD_API_KEY"
	DD_FORCE_PROFILE       = "DD_FORCE_PROFILE"
	DD_SITE                = "DD_SITE"
	NUM_GOROUTINES         = "NUM_GOROUTINES"
	PII_SCRUBBER_RULES     = "PII_SCRUBBER_RULES"
)

func Get(envVar string) string {
	return os.Getenv(envVar)
}

func Enabled(environmentVariable string) bool {
	return Get(environmentVariable) == "true"
}

func APMEnabled() bool {
	return Enabled(APM_ENABLED)
}
