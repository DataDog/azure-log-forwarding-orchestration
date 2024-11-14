package environment

import "os"

func Enabled(environmentVariable string) bool {
	return os.Getenv(environmentVariable) == "true"
}

func APMEnabled() bool {
	return Enabled("DD_APM_ENABLED")
}
