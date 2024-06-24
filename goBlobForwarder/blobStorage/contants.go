package blobStorage

var logContainerNames = []string{
	"insights-logs-functionapplogs",
	"insights-logs-operationallogs",
}

var AzureBlobURL = "https://%s.blob.core.windows.net/"
var cursorContainerName = "blob-cursor-cache"
var cursorBlobName = "dd_temp"

// CursorConfigs is a map of containerName/resourceName to byteoffset that we finished at:
//
//	{
//	 "insights-logs-functionapplogs": {
//		"resourceID1": "800byte",
//		"resourceID2": "250byte",
//	 },
//	 "insights-logs-operationallogs": {
//		"resourceId=/SUBSCRIPTIONS/xxx/RESOURCEGROUPS/xxx/PROVIDERS/MICROSOFT.WEB/SITES/xxx/": "300byte",
//	 }
//	}
type CursorConfigs map[string]map[string]string
