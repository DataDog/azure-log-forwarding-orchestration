package environment

import "os"

func Enabled(environmentVariable string) bool {
	return os.Getenv(environmentVariable) == "true"
}

func ApmEnabled() bool {
	return Enabled("DD_APM_ENABLED")
}
