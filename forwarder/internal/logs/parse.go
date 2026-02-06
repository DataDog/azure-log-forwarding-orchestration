// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

// EVOLVE-BLOCK-START
// This block controls imports - the AI can add/remove imports as needed
// Required imports are those used outside the Parse function
import (
	// stdlib - Required (used outside Parse function)
	"encoding/json" // Used in unmarshalFlowEventRecords and ActiveDirectoryParser
	"errors"        // Used in multiple parser implementations
	"slices"        // Used in Valid() methods

	// stdlib - These can be added/removed by AI as needed for Parse function
	"bufio" // For Scanner (required for function signature)
	"io"    // For ReadCloser (required for function signature)
	"iter"  // For Seq type (required for function signature)
	"sync"  // For Once, Pool and map initialization

	// 3p
	"github.com/dop251/goja/parser"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)
// EVOLVE-BLOCK-END

// ordered list of parsers, the first parser that returns true will be used
var parsers = []Parser{FlowEventParser{}, FunctionAppParser{}, ActiveDirectoryParser{}, AzureLogParser{}}

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

// FlowEventParser is a parser for flow events - vnet flow events and network security group flow events.
type FlowEventParser struct{}

func unmarshalFlowEventRecords[T any](bytes []byte) (*flowEventRecords[T], error) {
	var flowEventRecords flowEventRecords[T]
	err := json.Unmarshal(bytes, &flowEventRecords)
	return &flowEventRecords, err
}

func processFlowEventRecords[T flowEventRecord](flowEventRecords *flowEventRecords[T], blob storage.Blob, originalSize int, scrubbedSize int, piiScrubber Scrubber, yield func(ParsedLogResponse) bool) bool {
	response := ParsedLogResponse{}
	for idx, flowEventLog := range flowEventRecords.Records {
		currLog, err := flowEventLog.ToLog(blob)
		response.ParsedLog = currLog
		if err != nil {
			response.Err = err
			yield(response)
			return false
		}
		if idx == len(flowEventRecords.Records)-1 {
			response.ParsedLog.RawByteSize = int64(originalSize)
			response.ParsedLog.ScrubbedByteSize = int64(scrubbedSize)
		}

		if !yield(response) {
			return false
		}
	}
	return true
}

// Parse reads logs from a reader and parses them into Log objects.
func (f FlowEventParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq[ParsedLogResponse] {
	return func(yield func(ParsedLogResponse) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)
			scrubbedBytes := piiScrubber.Scrub(currBytes)
			scrubbedSize := len(scrubbedBytes)
			response := ParsedLogResponse{}

			switch blob.Container.Name {
			case NetworkSecurityGroupFlowEventContainer:
				networkSecGroupRecords, err := unmarshalFlowEventRecords[*networkSecurityGroupFlowLog](scrubbedBytes)
				if err != nil {
					response.Err = err
					yield(response)
					return
				}
				processFlowEventRecords[*networkSecurityGroupFlowLog](networkSecGroupRecords, blob, originalSize, scrubbedSize, piiScrubber, yield)
			case VnetFlowEventContainer:
				vnetFlowRecords, err := unmarshalFlowEventRecords[*vnetFlowEventLog](scrubbedBytes)
				if err != nil {
					response.Err = err
					yield(response)
					return
				}
				processFlowEventRecords[*vnetFlowEventLog](vnetFlowRecords, blob, originalSize, scrubbedSize, piiScrubber, yield)
			default:
				response.Err = errors.New("no parser found for log type" + blob.Container.Name)
				yield(response)
				return
			}
		}
	}
}

const (
	NetworkSecurityGroupFlowEventContainer = "insights-logs-networksecuritygroupflowevent"
	VnetFlowEventContainer                 = "insights-logs-flowlogflowevent"
)

var flowEventContainers = []string{VnetFlowEventContainer, NetworkSecurityGroupFlowEventContainer}

// Valid checks if the blob is in a flow event container.
func (f FlowEventParser) Valid(blob storage.Blob) bool {
	return slices.Contains(flowEventContainers, blob.Container.Name)
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

			// Note that function apps with a domain name set produce logs that do not have parsable resource IDs that
			// we can get a log source from. Set it explicitly.
			if len(currLog.Source) == 0 {
				currLog.Source = functionAppSource
				currLog.Tags = append(currLog.Tags, "source:"+functionAppSource)
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

type ActiveDirectoryParser struct{}

// TODO Support all AD log containers: https://datadoghq.atlassian.net/browse/AZINTS-3430
var activeDirectoryContainers = []string{
	"insights-logs-auditlogs",
	"insights-logs-signinlogs",
	"insights-logs-noninteractiveusersigninlogs",
	"insights-logs-serviceprincipalsigninlogs",
	"insights-logs-managedidentitysigninlogs",
	"insights-logs-riskyusers",
	"insights-logs-userriskevents",
	"insights-logs-microsoftgraphactivitylogs",
}

func (a ActiveDirectoryParser) Parse(scanner *bufio.Scanner, blob storage.Blob, piiScrubber Scrubber) iter.Seq[ParsedLogResponse] {
	return func(yield func(response ParsedLogResponse) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			originalSize := len(currBytes)
			scrubbedBytes := piiScrubber.Scrub(currBytes)

			var activeDirectoryLog activeDirectoryLog
			response := ParsedLogResponse{}
			err := json.Unmarshal(scrubbedBytes, &activeDirectoryLog)
			if err != nil {
				response.Err = err
				yield(response)
				return
			}
			currLog, err := activeDirectoryLog.ToLog(blob)

			if err != nil {
				response.Err = err
				yield(response)
				return
			}

			currLog.RawByteSize = int64(originalSize)
			currLog.ScrubbedByteSize = int64(len(scrubbedBytes))

			response.ParsedLog = currLog
			if !yield(response) {
				return
			}
		}
	}
}

func (a ActiveDirectoryParser) Valid(blob storage.Blob) bool {
	return slices.Contains(activeDirectoryContainers, blob.Container.Name)
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

// EVOLVE-BLOCK-START
// Parse reads logs from a reader and parses them into Log objects.
// It returns a sequence of ParsedLogResponse and a pointer to number of bytes read and an error if any.

var (
	parserMap        map[string]Parser
	parserMapOnce    sync.Once
	scannerBufferPool = sync.Pool{
		New: func() any {
			return make([]byte, initialBufferSize)
		},
	}
)

func initParserMap() {
	parserMap = make(map[string]Parser, 16)

	// Map flow event containers
	parserMap[NetworkSecurityGroupFlowEventContainer] = FlowEventParser{}
	parserMap[VnetFlowEventContainer] = FlowEventParser{}

	// Map function app container
	parserMap[functionAppContainer] = FunctionAppParser{}

	// Map all active directory containers
	for _, container := range activeDirectoryContainers {
		parserMap[container] = ActiveDirectoryParser{}
	}
}

func Parse(reader io.ReadCloser, blob storage.Blob, piiScrubber Scrubber) (iter.Seq[ParsedLogResponse], *int, error) {
	parserMapOnce.Do(initParserMap)

	var totalBytes int
	scanLines := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		currAdvance, token, err := bufio.ScanLines(data, atEOF)
		totalBytes += currAdvance
		return currAdvance, token, err
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(scanLines)

	// Container-aware buffer sizing
	var targetSize int
	switch blob.Container.Name {
	case NetworkSecurityGroupFlowEventContainer, VnetFlowEventContainer:
		targetSize = 512 * 1024 // 512KB for large flow events
	case functionAppContainer:
		targetSize = 64 * 1024 // 64KB for function app logs
	case "insights-logs-auditlogs", "insights-logs-signinlogs",
		"insights-logs-noninteractiveusersigninlogs", "insights-logs-serviceprincipalsigninlogs",
		"insights-logs-managedidentitysigninlogs", "insights-logs-riskyusers",
		"insights-logs-userriskevents", "insights-logs-microsoftgraphactivitylogs":
		targetSize = 64 * 1024 // 64KB for Active Directory logs
	default:
		targetSize = initialBufferSize // default 64KB
	}

	// Get buffer from pool
	buffer := scannerBufferPool.Get().([]byte)

	// If pooled buffer is too small for this container type, allocate a larger one
	if cap(buffer) < targetSize {
		buffer = make([]byte, targetSize)
	}

	scanner.Buffer(buffer[:targetSize], maxBufferSize)

	// Fast path: O(1) lookup in parser map
	var parserUsed Parser
	var found bool
	if parserUsed, found = parserMap[blob.Container.Name]; !found {
		// Fallback: linear search for unknown containers (maintains compatibility)
		for _, p := range parsers {
			if p.Valid(blob) {
				parserUsed = p
				found = true
				break
			}
		}
		if !found {
			scannerBufferPool.Put(buffer)
			return nil, &totalBytes, errors.New("no parser found for blob")
		}
	}

	seq := parserUsed.Parse(scanner, blob, piiScrubber)

	// Wrap the sequence to return buffer to pool after iteration completes
	return func(yield func(ParsedLogResponse) bool) {
		defer func() {
			// Only pool buffers up to 2x initialBufferSize to prevent memory bloat
			if cap(buffer) <= 2*initialBufferSize {
				scannerBufferPool.Put(buffer)
			}
		}()
		seq(yield)
	}, &totalBytes, nil
}
// EVOLVE-BLOCK-END
