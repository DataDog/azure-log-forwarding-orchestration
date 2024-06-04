package main

import (
	"context"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/FormatAzureLogs"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"log"
	"os"
)

func run(ctx context.Context, data []byte) {
	if formatAzureLogs.DdApiKey == "" || formatAzureLogs.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}

	// format the logs
	handler := formatAzureLogs.NewBlobLogFormatter(ctx)
	azureLogs, totalSize := handler.ParseBlobData(data)
	// batch logs to avoid sending too many logs in a single request
	batcher := formatAzureLogs.NewBatcher(256*1000, 4*1000*1000, 400)
	formatedLogs := batcher.Batch(azureLogs, totalSize)

	// scrub the logs
	scrubberConfig := []formatAzureLogs.ScrubberRuleConfigs{
		{
			"REDACT_IP": {
				Pattern:     `[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}`,
				Replacement: "xxx.xxx.nn.xxx",
			},
			"REDACT_EMAIL": {
				Pattern:     `[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+`,
				Replacement: "xx@nn.co",
			},
		},
	}

	// submit logs to Datadog
	err := formatAzureLogs.NewHTTPClient(context.TODO(), scrubberConfig).SendAll(formatedLogs)
	if err != nil {
		return
	}

}

func testLocalFile() {
	m := map[string]interface{}{
		"log": log.New(os.Stderr, "", 0),
		"executionContext": map[string]string{
			"functionName": "test",
		},
		"done": func() {},
	}
	log.Println(m)

	//data, err := os.ReadFile("/Users/nina.rei/Downloads/azure_op_0530.json")
	data, err := os.ReadFile("/Users/nina.rei/Downloads/azure_op_530.json")
	if err != nil {
		log.Fatal(err)
	}

	run(context.Background(), data)
}

func main() {
	client := blobCache.NewBlobClient(context.Background(), formatAzureLogs.StorageAccount)
	//initialize container for cursor cache
	//testLocalFile()
	client.BlobC()
	//get logs as byte array
	data := client.GetLogsFromSpecificBlobContainer("insights-logs-functionapplogs")
	//parse and send logs
	run(context.Background(), data)
}
