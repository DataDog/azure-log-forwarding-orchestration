package environment

import "os"

func Enabled(environmentVariable string) bool {
	return os.Getenv(environmentVariable) == "true"
}
