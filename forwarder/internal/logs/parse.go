package logs

import (
	// stdlib
	"bufio"
	"encoding/json"
	"io"
	"iter"
	"strings"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

var parsers = []Parser{FlowEventParser{}, FunctionAppParser{}, AzureLogParser{}}

// Parser is an interface for parsing logs.
type Parser interface {
	Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) (iter.Seq2[*Log, error], bool)
}

// FlowEventParser is a parser for flow events.
type FlowEventParser struct{}

func (f FlowEventParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) (iter.Seq2[*Log, error], bool) {
	if blob.Container.Name != flowEventContainer {
		return nil, false
	}
	parsingFunc := func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
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
	}

	return parsingFunc, true
}

type FunctionAppParser struct{}

func (f FunctionAppParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) (iter.Seq2[*Log, error], bool) {
	if blob.Container.Name != functionAppContainer {
		return nil, false
	}
	parsingFunc := func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)

			currBytes, err := BytesFromJavaScriptObject(currBytes)
			if err != nil {
				if strings.Contains(err.Error(), "Unexpected token ;") {
					if !yield(nil, ErrUnexpectedToken) {
						return
					}
				}
				if !yield(nil, err) {
					return
				}
			}

			scrubbedBytes := piiScrubber.Scrub(&currBytes)
			currLog, err := NewLog(*scrubbedBytes, blob, piiScrubber, int64(originalSize))
			if err != nil && !yield(nil, err) {
				return
			}
			currLog.RawByteSize = int64(originalSize)
			if !yield(currLog, nil) {
				return
			}
		}
	}

	return parsingFunc, true
}

type AzureLogParser struct{}

func (a AzureLogParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) (iter.Seq2[*Log, error], bool) {
	parsingFunc := func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)
			currLog, err := NewLog(currBytes, blob, piiScrubber, int64(originalSize))
			if err != nil && !yield(nil, err) {
				return
			}
			if !yield(currLog, nil) {
				return
			}
		}
	}
	return parsingFunc, true
}

// Parse reads logs from a reader and parses them into Log objects.
func Parse(reader io.ReadCloser, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	// iterate over parsers
	for _, parser := range parsers {
		iterator, ok := parser.Parse(scanner, blob, piiScrubber)
		if ok {
			return iterator
		}
	}
	return nil
}
