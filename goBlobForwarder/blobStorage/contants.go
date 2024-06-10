package blobStorage

var logContainerNames = []string{
	"insights-logs-functionapplogs",
	"insights-logs-operationallogs",
}

var azureBlobURL = "https://%s.blob.core.windows.net/"
var cursorContainerName = "blob-cursor-cache"
var cursorBlobName = "dd_temp"

// CursorConfigs is a map of containerName/resourceName to byteoffset that we finished at:
//
//	{
//	 "insights-logs-functionapplogs": {
//		"resource1": "800byte",
//		"resource2": "250byte",
//	 },
//	 "insights-logs-operationallogs": {
//		"resource1": "300byte",
//	 }
//	}
type CursorConfigs map[string]map[string]string
