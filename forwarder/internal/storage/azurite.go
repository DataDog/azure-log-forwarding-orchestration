package storage

import (
	"os"
	"path"
	"strings"
)

func GetAzuriteFixturesPath() (string, error) {
	basePath, err := os.Getwd()
	if err != nil {
		return "", err
	}
	if !strings.Contains(basePath, "cmd/forwarder") {
		basePath = path.Join(basePath, "cmd", "forwarder")
	}
	filePath := path.Join(basePath, "fixtures", "azurite")
	return filePath, nil
}
