// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

import (
	// stdlib
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	log "github.com/sirupsen/logrus"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
	customtime "github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/time"
)

var supportedTimeLayouts []string

func init() {
	// Add the supported time layouts for parsing Azure logs
	supportedTimeLayouts = []string{
		time.RFC3339,          // ISO 8601 format,
		"01/02/2006 15:04:05", // USA short timestamp format
		"1/2/2006 3:04:05 PM", // USA short timestamp format with AM/PM
	}
}

func parseTime(timeString string) (parsedTime time.Time, timeParsingErrors error) {
	timeString = strings.TrimSpace(timeString) // Trim leading and trailing whitespace
	for _, layout := range supportedTimeLayouts {
		var currErr error
		parsedTime, currErr = time.Parse(layout, timeString)
		if currErr == nil {
			timeParsingErrors = nil
			break // Successfully parsed the time
		}
		timeParsingErrors = errors.Join(timeParsingErrors, currErr)
	}
	return
}

// Log represents a log to send to Datadog.
type Log struct {
	Content          []byte
	RawByteSize      int64
	ScrubbedByteSize int64
	Tags             []string
	Category         string
	Container        string
	Blob             string
	ResourceId       string
	Service          string
	Source           string
	Time             time.Time
	Level            string
}

// NewLog creates a new Log from the given log bytes.
func NewLog(logBytes []byte, blob storage.Blob, scrubber Scrubber, originalSize int64) (*Log, error) {
	var currLog *azureLog

	if blob.IsJson() {
		err := json.Unmarshal(logBytes, &currLog)
		if err != nil {
			if err.Error() == "unexpected end of JSON input" {
				return nil, ErrIncompleteLogFile
			}
			return nil, err
		}
	} else {
		currLog = &azureLog{}
	}

	blobNameResourceId := blob.ResourceId()
	currLog.blobResourceId = blobNameResourceId
	currLog.byteSize = originalSize
	currLog.raw = logBytes
	currLog.Container = blob.Container.Name
	currLog.Blob = blob.Name

	return currLog.ToLog(scrubber)
}

// RawLength returns the length of the original Azure log content.
func (l *Log) RawLength() int64 {
	return l.RawByteSize
}

// ScrubbedLength returns the length of the log content after it has been scrubbed for PII.
func (l *Log) ScrubbedLength() int64 {
	return l.ScrubbedByteSize
}

// Validate checks if the log is valid to send to Datadog.
func (l *Log) Validate(now customtime.Now, logger *log.Entry) bool {
	return validateLog(l.ResourceId, l.ScrubbedByteSize, l.Time, now, logger)
}

type azureLog struct {
	raw             []byte
	byteSize        int64
	Category        string `json:"category"`
	Container       string `json:"container"`
	Blob            string `json:"blob"`
	ResourceIdLower string `json:"resourceId,omitempty"`
	ResourceIdUpper string `json:"ResourceId,omitempty"`
	// resource ID from blob name, used as a backup
	blobResourceId string
	TimeString     string `json:"time"`
	Level          string `json:"level,omitempty"`
}

func (l *azureLog) ResourceId() *arm.ResourceID {
	for _, resourceId := range []string{l.ResourceIdLower, l.ResourceIdUpper, l.blobResourceId} {
		if r, err := arm.ParseResourceID(resourceId); err == nil {
			return r
		}
	}
	return nil
}

func (l *azureLog) ToLog(scrubber Scrubber) (*Log, error) {
	tags := append([]string(nil), DefaultTags...)

	var logSource string
	var resourceId string

	// Try to add additional tags, source, and resource ID
	if parsedId := l.ResourceId(); parsedId != nil {
		logSource = sourceTag(parsedId.ResourceType.String())
		resourceId = parsedId.String()
		tags = append(tags, tagsFromResourceId(parsedId)...)
	}

	if l.Level == "" {
		l.Level = "Informational"
	}

	var parsedTime time.Time
	var timeParsingErrors error

	if l.TimeString == "" {
		// No time string found in the log, use current time
		parsedTime = time.Now().UTC()
	} else {
		parsedTime, timeParsingErrors = parseTime(l.TimeString)
	}
	if (parsedTime.IsZero()) && l.TimeString != "" {
		if timeParsingErrors != nil {
			return nil, fmt.Errorf("unable to parse time: %w", timeParsingErrors)
		}
		return nil, errors.New("time is zero but we had no parsing errors, this is unexpected")
	}

	scrubbedLog := scrubber.Scrub(l.raw)
	scrubbedByteSize := len(scrubbedLog) + newlineBytes // need to account for scrubed and raw log size so cursors remain accurate

	return &Log{
		Content:          scrubbedLog,
		RawByteSize:      l.byteSize,
		ScrubbedByteSize: int64(scrubbedByteSize),
		Category:         l.Category,
		ResourceId:       resourceId,
		Service:          azureService,
		Source:           logSource,
		Time:             parsedTime,
		Level:            l.Level,
		Tags:             tags,
		Container:        l.Container,
		Blob:             l.Blob,
	}, nil
}

// Active Directory (AD) logs are made up of numerous log categories,
// so parse out a few key fields and submit rest as generic JSON
type activeDirectoryLog map[string]json.RawMessage

const (
	azureActiveDirectorySource = "azure.aadiam"
	userRiskEventsLogContainer = "insights-logs-userriskevents"
	riskyUsersLogContainer     = "insights-logs-riskyusers"
)

// These AD log containers have a `time` field that is not in standard RFC3339 format
var usaShortTimestampLogContainers = []string{userRiskEventsLogContainer, riskyUsersLogContainer}

func (adl *activeDirectoryLog) Bytes() ([]byte, error) {
	return json.Marshal(adl)
}

func (adl *activeDirectoryLog) ToLog(blob storage.Blob) (*Log, error) {
	logBytes, err := adl.Bytes()
	if err != nil {
		return nil, err
	}

	var logTime time.Time
	if slices.Contains(usaShortTimestampLogContainers, blob.Container.Name) {
		var timeString string
		value, ok := (*adl)["time"]
		if !ok {
			return nil, errors.New("'time' key is missing")
		}
		timeString = strings.Trim(string(value), "\"")

		logTime, err = parseTime(timeString)
		if err != nil {
			return nil, err
		}
	} else {
		err = json.Unmarshal((*adl)["time"], &logTime)
		if err != nil {
			return nil, err
		}
	}

	var category string
	value, ok := (*adl)["category"]
	if !ok {
		return nil, errors.New("'category' key is missing")
	}
	category = strings.Trim(string(value), "\"")

	var resourceId string
	value, ok = (*adl)["resourceId"]
	if !ok {
		return nil, errors.New("'resourceId' key is missing")
	}
	resourceId = strings.Trim(string(value), "\"")

	tenantIdFromResourceId := func(adResourceId string) (string, error) {
		// Sample active directory resource ID: /tenants/00000000-0000-0000-0000-000000000000/providers/Microsoft.aadiam
		// Don't use arm.ParseResourceID here because active directory is not an ARM resource

		trimmed := strings.Trim(adResourceId, "/")
		parts := strings.Split(trimmed, "/")

		if len(parts) < 2 {
			return "", errors.New("unable to get tenant ID from AD resource ID")
		}

		if !strings.EqualFold(parts[0], "tenants") {
			return "", errors.New("unexpected resource ID - ID is tenant-scoped")
		}

		return parts[1], nil
	}

	tags := append([]string{"source:" + azureActiveDirectorySource}, DefaultTags...)
	tenantId, err := tenantIdFromResourceId(resourceId)
	if err != nil {
		return nil, err
	}

	tags = append(tags, "tenant_id:"+tenantId)

	return &Log{
		Time:       logTime,
		Category:   category,
		ResourceId: resourceId,
		Service:    azureService,
		Source:     azureActiveDirectorySource,
		Content:    logBytes,
		Container:  blob.Container.Name,
		Blob:       blob.Name,
		Level:      "Informational",
		Tags:       tags,
	}, nil
}

type vnetFlowLog struct {
	Time          time.Time `json:"time"`
	SystemID      string    `json:"systemId"`
	MacAddress    string    `json:"macAddress"`
	Category      string    `json:"category"`
	ResourceID    string    `json:"resourceId"`
	OperationName string    `json:"operationName"`
	Properties    struct {
		Version int `json:"Version"`
		Flows   []struct {
			Rule  string `json:"rule"`
			Flows []struct {
				Mac        string   `json:"mac"`
				FlowTuples []string `json:"flowTuples"`
			} `json:"flows"`
		} `json:"flows"`
	} `json:"properties"`
}

type vnetFlowLogs struct {
	Records []vnetFlowLog `json:"records"`
}

func (l *vnetFlowLog) Bytes() ([]byte, error) {
	return json.Marshal(l)
}

func (l *vnetFlowLog) ToLog(blob storage.Blob) (*Log, error) {
	logBytes, err := l.Bytes()
	if err != nil {
		return nil, err
	}

	parsedId, err := arm.ParseResourceID(l.ResourceID)
	if err != nil && l.ResourceID != "" {
		return nil, err
	}

	tags := append([]string(nil), DefaultTags...)

	var logSource string
	if parsedId != nil {
		logSource = sourceTag(parsedId.ResourceType.String())
	}
	tags = append(tags, tagsFromResourceId(parsedId)...)

	return &Log{
		Time:       l.Time,
		Category:   l.Category,
		ResourceId: l.ResourceID,
		Service:    azureService,
		Source:     logSource,
		Content:    logBytes,
		Container:  blob.Container.Name,
		Blob:       blob.Name,
		Level:      "Informational",
		Tags:       tags,
	}, nil
}
