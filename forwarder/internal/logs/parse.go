// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

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

// ParsedLogResponse is the response type for parsers
type ParsedLogResponse struct {
	ParsedLog *Log
	Err       error
}

// Parser is an interface for parsing logs.
type Parser interface {
	Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq[ParsedLogResponse]
	Valid(blob storage.Blob) bool
}

// FlowEventParser is a parser for flow events.
type FlowEventParser struct{}

// Parse reads logs from a reader and parses them into Log objects.
func (f FlowEventParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq[ParsedLogResponse] {
	return func(yield func(ParsedLogResponse) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			var flowLogs vnetFlowLogs
			originalSize := len(currBytes)
			scrubbedBytes := piiScrubber.Scrub(currBytes)

			response := ParsedLogResponse{}

			err := json.Unmarshal(scrubbedBytes, &flowLogs)
			if err != nil {
				response.Err = err
				yield(response)
				return
			}
			for idx, flowLog := range flowLogs.Records {
				currLog, err := flowLog.ToLog(blob)
				if err != nil {
					response.Err = err
					yield(response)
					return
				}
				if idx == len(flowLogs.Records)-1 {
					currLog.RawByteSize = int64(originalSize)
				}
				response.ParsedLog = currLog
				if !yield(response) {
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

func (f FunctionAppParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq[ParsedLogResponse] {
	return func(yield func(ParsedLogResponse) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)

			parsedBytes, err := BytesFromJavaScriptObject(currBytes)
			response := ParsedLogResponse{}
			if err != nil {
				if errors.As(err, &parser.ErrorList{}) || errors.As(err, &parser.Error{}) {
					response.Err = errors.Join(ErrUnexpectedToken, err)
					yield(response)
					return
				}
				response.Err = err
				yield(response)
				return
			}

			scrubbedBytes := piiScrubber.Scrub(parsedBytes)
			currLog, err := NewLog(scrubbedBytes, blob, piiScrubber, int64(originalSize))
			if err != nil {
				response.Err = err
				yield(response)
				return
			}
			currLog.RawByteSize = int64(originalSize)
			response.ParsedLog = currLog
			if !yield(response) {
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

func (a AzureLogParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq[ParsedLogResponse] {
	return func(yield func(response ParsedLogResponse) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)
			currLog, err := NewLog(currBytes, blob, piiScrubber, int64(originalSize))
			response := ParsedLogResponse{}
			if err != nil {
				response.Err = err
				yield(response)
				return
			}
			response.ParsedLog = currLog
			if !yield(response) {
				return
			}
		}
	}
}

// Valid is always true for AzureLogParser.
func (a AzureLogParser) Valid(blob storage.Blob) bool {
	return true
}

type advanceCounter struct {
	value int
}

func (a *advanceCounter) Add(delta int) {
	a.value += delta
}

func (a *advanceCounter) Get() int {
	return a.value
}

// NewAdvanceCounter creates a new AdvanceCounter.
func newAdvanceCounter() *advanceCounter {
	return &advanceCounter{
		value: 0,
	}
}

type ValueCounter interface {
	Get() int
}

// Parse reads logs from a reader and parses them into Log objects.
// It returns a sequence of ParsedLogResponse and a function to get the number of newline bytes read and an error if any.
func Parse(reader io.ReadCloser, blob storage.Blob, piiScrubber Scrubber) (iter.Seq[ParsedLogResponse], ValueCounter, error) {
	aCounter := newAdvanceCounter()

	scanLines := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		currAdvance, token, err := bufio.ScanLines(data, atEOF)
		aCounter.Add(currAdvance)
		return currAdvance, token, err
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(scanLines)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)
	// iterate over parsers
	for _, parser := range parsers {
		if parser.Valid(blob) {
			return parser.Parse(scanner, blob, piiScrubber), aCounter, nil
		}
	}

	return nil, aCounter, errors.New("no parser found for blob")
}
