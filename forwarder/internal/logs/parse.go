package logs

import (
	// stdlib
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"iter"

	// 3p
	"github.com/dop251/goja/parser"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// ordered list of parsers, the first parser that returns true will be used
var parsers = []Parser{FlowEventParser{}, FunctionAppParser{}, AzureLogParser{}}

// Parser is an interface for parsing logs.
type Parser interface {
	Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error]
	Valid(blob storage.Blob) bool
}

// FlowEventParser is a parser for flow events.
type FlowEventParser struct{}

// Parse reads logs from a reader and parses them into Log objects.
func (f FlowEventParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			var flowLogs vnetFlowLogs
			originalSize := len(currBytes)
			scrubbedBytes := piiScrubber.Scrub(currBytes)
			err := json.Unmarshal(scrubbedBytes, &flowLogs)
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
}

// Valid checks if the blob is in a flow event container.
func (f FlowEventParser) Valid(blob storage.Blob) bool {
	return blob.Container.Name == flowEventContainer
}

type FunctionAppParser struct{}

func (f FunctionAppParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)

			currBytes, err := BytesFromJavaScriptObject(currBytes)
			if err != nil {
				if errors.As(err, &parser.ErrorList{}) || errors.As(err, &parser.Error{}) {
					if !yield(nil, errors.Join(ErrUnexpectedToken, err)) {
						return
					}
				}
				if !yield(nil, err) {
					return
				}
			}

			scrubbedBytes := piiScrubber.Scrub(currBytes)
			currLog, err := NewLog(scrubbedBytes, blob, piiScrubber, int64(originalSize))
			if err != nil && !yield(nil, err) {
				return
			}
			currLog.RawByteSize = int64(originalSize)
			if !yield(currLog, nil) {
				return
			}
		}
	}
}

// Valid checks if the blob is in a function app container.
func (f FunctionAppParser) Valid(blob storage.Blob) bool {
	return blob.Container.Name == functionAppContainer
}

type AzureLogParser struct{}

func (a AzureLogParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	return func(yield func(*Log, error) bool) {
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
}

// Valid is always true for AzureLogParser.
func (a AzureLogParser) Valid(blob storage.Blob) bool {
	return true
}

// Parse reads logs from a reader and parses them into Log objects.
func Parse(reader io.ReadCloser, blob storage.Blob, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	// iterate over parsers
	for _, parser := range parsers {
		if parser.Valid(blob) {
			return parser.Parse(scanner, blob, piiScrubber)
		}
	}

	return func(yield func(*Log, error) bool) {
		yield(nil, errors.New("no parser found for blob"))
	}
}
