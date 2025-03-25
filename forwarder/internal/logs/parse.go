package logs

import (
	// stdlib
	"bufio"
	"encoding/json"
	"io"
	"iter"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

type Parser interface {
	Parse(reader io.ReadCloser, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error]
}

// Parse reads logs from a reader and parses them into Log objects.
func Parse(reader io.ReadCloser, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	return func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			if blob.Container.Name == flowEventContainer {
				var flowLogs vnetFlowLogs
				originalSize := len(currBytes)
				scrubbedBytes := piiScrubber.Scrub(&currBytes)
				err := json.Unmarshal(*scrubbedBytes, &flowLogs)
				if err != nil {
					if !yield(nil, err) {
						return
					}
				}
				for idx, flowLog := range flowLogs.Records {
					currLog, err := flowLog.ToLog(blob)
					if err != nil && !yield(nil, err) {
						return
					}
					if idx == len(flowLogs.Records)-1 {
						currLog.RawByteSize = int64(originalSize)
					}
					if !yield(currLog, nil) {
						return
					}
				}
				continue
			}
			currLog, err := NewLog(currBytes, blob, piiScrubber)
			if err != nil && !yield(nil, err) {
				return
			}
			if !yield(currLog, nil) {
				return
			}
		}
	}

}
