// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package logs

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// FlowEventResponse includes the position immediately after a complete record in
// the original blob. Unlike EOF, this position is stable when Azure inserts more
// records before the closing array/object delimiters of an hourly flow-log blob.
type FlowEventResponse struct {
	ParsedLogResponse
	EndOffset int64
}

// ParseFlowEvents reads complete flow-log documents from the beginning of a blob,
// skipping records at or before the persisted byte checkpoint. Reading from the
// checkpoint itself would omit the JSON envelope (and can start inside a record
// when migrating an old EOF checkpoint). Multiple documents are supported for
// compatibility with newline-delimited flow logs.
func ParseFlowEvents(reader io.Reader, blob storage.Blob, piiScrubber Scrubber, checkpoint int64) iter.Seq[FlowEventResponse] {
	return func(yield func(FlowEventResponse) bool) {
		decoder := json.NewDecoder(reader)
		fail := func(err error) {
			yield(FlowEventResponse{ParsedLogResponse: ParsedLogResponse{Err: err}})
		}
		for {
			token, err := decoder.Token()
			if err == io.EOF {
				return
			}
			if err != nil {
				fail(err)
				return
			}
			if token != json.Delim('{') {
				fail(fmt.Errorf("expected flow-log object, got %v", token))
				return
			}
			foundRecords := false
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					fail(err)
					return
				}
				if key != "records" {
					var ignored json.RawMessage
					if err := decoder.Decode(&ignored); err != nil {
						fail(err)
						return
					}
					continue
				}
				foundRecords = true
				if err := expectFlowDelimiter(decoder, '['); err != nil {
					fail(err)
					return
				}
				for decoder.More() {
					var raw json.RawMessage
					if err := decoder.Decode(&raw); err != nil {
						fail(err)
						return
					}
					end := decoder.InputOffset()
					if end <= checkpoint {
						continue
					}
					parsed, err := parseFlowEventRecord(raw, blob, piiScrubber)
					if err != nil {
						fail(err)
						return
					}
					if !yield(FlowEventResponse{ParsedLogResponse: ParsedLogResponse{ParsedLog: parsed}, EndOffset: end}) {
						return
					}
				}
				if err := expectFlowDelimiter(decoder, ']'); err != nil {
					fail(err)
					return
				}
			}
			if err := expectFlowDelimiter(decoder, '}'); err != nil {
				fail(err)
				return
			}
			if !foundRecords {
				fail(fmt.Errorf("flow-log object has no records array"))
				return
			}
		}
	}
}

func expectFlowDelimiter(decoder *json.Decoder, delimiter json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != delimiter {
		return fmt.Errorf("expected flow-log delimiter %q, got %v", delimiter, token)
	}
	return nil
}

func parseFlowEventRecord(raw []byte, blob storage.Blob, piiScrubber Scrubber) (*Log, error) {
	scrubbed := piiScrubber.Scrub(raw)
	var record flowEventRecord
	switch blob.Container.Name {
	case VnetFlowEventContainer:
		record = &vnetFlowEventLog{}
	case NetworkSecurityGroupFlowEventContainer:
		record = &networkSecurityGroupFlowLog{}
	default:
		return nil, fmt.Errorf("unsupported flow-log container %q", blob.Container.Name)
	}
	if err := json.Unmarshal(scrubbed, record); err != nil {
		return nil, err
	}
	parsed, err := record.ToLog(blob)
	if err != nil {
		return nil, err
	}
	parsed.RawByteSize = int64(len(raw))
	parsed.ScrubbedByteSize = int64(len(scrubbed))
	return parsed, nil
}
