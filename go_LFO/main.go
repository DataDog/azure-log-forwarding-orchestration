package main

import (
	"context"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/FormatAzureLogs"
	"github.com/DataDog/azure-log-forwarding-offering/go_LFO/blobCache"
	"log"
	"os"
)

func run(ctx context.Context, data []byte) {
	if FormatAzureLogs.DdApiKey == "" || FormatAzureLogs.DdApiKey == "<DATADOG_API_KEY>" {
		log.Println("You must configure your API key before starting this function (see ## Parameters section)")
		return
	}

	// format the logs
	handler := FormatAzureLogs.NewBlobLogFormatter(ctx)
	azureLogs, totalSize := handler.ParseBlobData(data)
	// batch logs to avoid sending too many logs in a single request
	batcher := FormatAzureLogs.NewBatcher(256*1000, 4*1000*1000, 400)
	formatedLogs := batcher.Batch(azureLogs, totalSize)

	// scrub the logs
	scrubberConfig := []FormatAzureLogs.ScrubberRuleConfigs{
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
	err := FormatAzureLogs.NewHTTPClient(context.TODO(), scrubberConfig).SendAll(formatedLogs)
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
	//logs := getTheLogs()
	blobCache.BlobC()
	//testLocalFile()
	//formattedLogs
}
