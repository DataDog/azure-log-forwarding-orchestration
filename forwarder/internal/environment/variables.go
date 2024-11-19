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
)

func Enabled(environmentVariable string) bool {
	return GetEnvVar(environmentVariable) == "true"
}

func APMEnabled() bool {
	return Enabled(APM_ENABLED)
}

func GetEnvVar(varName string) string {
	return os.Getenv(varName)
}
