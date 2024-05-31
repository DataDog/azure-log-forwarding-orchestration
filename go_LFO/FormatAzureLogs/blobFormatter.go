package FormatAzureLogs

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
)

type BlobLogFormatter struct {
	Context            context.Context
	logSplittingConfig interface{}
}

func NewBlobLogFormatter(context context.Context) BlobLogFormatter {
	return BlobLogFormatter{
		Context:            context,
		logSplittingConfig: getLogSplittingConfig(),
	}
}

func getAzureLogFieldsFromJson(logStruct *AzureLogs, tempJson map[string]json.RawMessage) {
	if err := json.Unmarshal(tempJson["resourceId"], &logStruct.DDRequire.ResourceId); err != nil {
		panic(err)
	}
	delete(tempJson, "resourceId")

	if err := json.Unmarshal(tempJson["category"], &logStruct.DDRequire.Category); err != nil {
		panic(err)
	}
	delete(tempJson, "category")
}

func unmarshallToPartialStruct(azureLog []byte) AzureLogs {
	var err error
	// partially unmarshall json to struct and keep remaining data as Raw json
	var azureStruct AzureLogs
	tempJson := make(map[string]json.RawMessage)
	if err = json.Unmarshal(azureLog, &tempJson); err != nil {
		panic(err)
	}

	getAzureLogFieldsFromJson(&azureStruct, tempJson)

	azureStruct.Rest, err = json.Marshal(tempJson)
	if err != nil {
		panic(err)
	}
	return azureStruct
}

func (b *BlobLogFormatter) ParseBlobData(data []byte) ([]AzureLogs, int) {
	var parsedLogs []AzureLogs
	var totalSize int
	logs := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	for _, azureLog := range logs {
		azureLog = bytes.ReplaceAll(azureLog, []byte("'"), []byte("\""))
		azureStruct := unmarshallToPartialStruct(azureLog)
		addTagsToJsonLog(&azureStruct)

		azureStruct.ByteSize = len(azureLog)
		totalSize += azureStruct.ByteSize
		parsedLogs = append(parsedLogs, azureStruct)
	}
	return parsedLogs, totalSize
}

func createDDTags(tags []string, name string) string {
	forwarderVersionTag := "forwarderversion:" + VERSION
	tags = append(tags, forwarderVersionTag)

	if name != "" {
		forwarderNameTag := "forwardername:" + name
		tags = append(tags, forwarderNameTag)
	}
	if DdTags != "" {
		tags = append(tags, DdTags)
	}

	ddTags := strings.Join(tags, ",")
	return ddTags
}

func addTagsToJsonLog(record *AzureLogs) {
	source, tags := parseResourceIdArray(record.DDRequire.ResourceId)
	record.DDRequire.Ddsource = source
	record.DDRequire.Ddtags = createDDTags(tags, record.ForwarderName)

	record.DDRequire.Ddsourcecategory = DdSourceCategory
	record.DDRequire.Service = DdService
}

func parseResourceIdArray(resourceId string) (source string, tags []string) {
	// Convert a valid resource ID to an array, handling beginning/ending slashes
	resourceIdArray := strings.Split(strings.ToLower(strings.TrimSpace(resourceId)), "/")

	for i := range resourceIdArray {
		switch resourceIdArray[i] {
		case "subscriptions":
			i += 1
			tags = append(tags, "subscription_id:"+resourceIdArray[i])
		case "providers":
			i += 1
			// looks for the source inside the resourceID PROVIDERS/MICROSOFT.WEB and wakes everything after .
			source = "azure." + strings.Split(resourceIdArray[i], ".")[1]
		case "resourcegroups":
			i += 1
			tags = append(tags, "resource_group:"+resourceIdArray[i])
		}
	}
	if source == "" {
		source = DdSource
	}
	return source, tags
}
