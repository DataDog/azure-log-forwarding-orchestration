package logs

import (
	// stdlib
	"encoding/json"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	log "github.com/sirupsen/logrus"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// Log represents a log to send to Datadog.
type Log struct {
	content          []byte
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
		currLog = &azureLog{Time: time.Now()}
	}

	blobNameResourceId, _ := blob.ResourceId()
	currLog.BlobResourceId = blobNameResourceId
	currLog.ByteSize = originalSize + newlineBytes
	currLog.Raw = logBytes
	currLog.Container = blob.Container.Name
	currLog.Blob = blob.Name

	return currLog.ToLog(scrubber), nil
}

// Content converts the log content to a string.
func (l *Log) Content() string {
	if l.content == nil {
		return ""
	}
	return string(l.content)
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
func (l *Log) Validate(logger *log.Entry) bool {
	return validateLog(l.ResourceId, l.ScrubbedByteSize, l.Time, logger)
}

type azureLog struct {
	Raw             []byte
	ByteSize        int64
	Category        string `json:"category"`
	Container       string `json:"container"`
	Blob            string `json:"blob"`
	ResourceIdLower string `json:"resourceId,omitempty"`
	ResourceIdUpper string `json:"ResourceId,omitempty"`
	// resource ID from blob name, used as a backup
	BlobResourceId string
	Time           time.Time `json:"time"`
	Level          string    `json:"level,omitempty"`
}

func (l *azureLog) ResourceId() *arm.ResourceID {
	for _, resourceId := range []string{l.ResourceIdLower, l.ResourceIdUpper, l.BlobResourceId} {
		if r, err := arm.ParseResourceID(resourceId); err == nil {
			return r
		}
	}
	return nil
}

func (l *azureLog) ToLog(scrubber Scrubber) *Log {
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

	scrubbedLog := scrubber.Scrub(l.Raw)
	scrubbedByteSize := len(scrubbedLog) + newlineBytes // need to account for scrubed and raw log size so cursors remain accurate

	return &Log{
		content:          scrubbedLog,
		RawByteSize:      l.ByteSize,
		ScrubbedByteSize: int64(scrubbedByteSize),
		Category:         l.Category,
		ResourceId:       resourceId,
		Service:          AzureService,
		Source:           logSource,
		Time:             l.Time,
		Level:            l.Level,
		Tags:             tags,
		Container:        l.Container,
		Blob:             l.Blob,
	}
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
		Service:    AzureService,
		Source:     logSource,
		content:    logBytes,
		Container:  blob.Container.Name,
		Blob:       blob.Name,
		Level:      "Informational",
		Tags:       tags,
	}, nil
}
