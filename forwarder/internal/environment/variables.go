package environment

import (
	"os"
)

// Environment variable names
const (
	ApmEnabled          = "DD_APM_ENABLED"
	AzureWebJobsStorage = "AzureWebJobsStorage"
	ControlPlaneId      = "CONTROL_PLANE_ID"
	ConfigId            = "CONFIG_ID"
	DdApiKey            = "DD_API_KEY"
	ForceProfile        = "DD_FORCE_PROFILE"
	DdSite              = "DD_SITE"
	TelemetryEnabled    = "DD_TELEMETRY"
	NumGoroutines       = "NUM_GOROUTINES"
	PiiScrubberRules    = "PII_SCRUBBER_RULES"
)

func Get(envVar string) string {
	return os.Getenv(envVar)
}

func Enabled(environmentVariable string) bool {
	return Get(environmentVariable) == "true"
}

func APMEnabled() bool {
	return Enabled(ApmEnabled)
}
