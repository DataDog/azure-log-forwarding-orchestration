package logs

import (
	// stdlib
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"math"
	"net/http"
	"strings"
	"time"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/dop251/goja/ast"
	"github.com/dop251/goja/parser"
	log "github.com/sirupsen/logrus"

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/pointer"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

var (
	// DefaultTags are the tags to include with every log.
	DefaultTags = []string{
		"forwarder:lfo",
		"control_plane_id:%" + environment.Get(environment.ControlPlaneId),
		"config_id:%s" + environment.Get(environment.ConfigId),
	}
	sourceTagMap map[string]string
)

const (
	AzureService = "azure"

	// maxBufferSize is the maximum buffer to use for scanning logs.
	// Logs greater than this buffer will be dropped by bufio.Scanner.
	// The buffer is defaulted to the maximum value of an integer.
	maxBufferSize = math.MaxInt32

	// initialBufferSize is the initial buffer size to use for scanning logs.
	initialBufferSize = 1024 * 1024 * 5

	// newlineBytes is the number of bytes in a newline character in utf-8.
	newlineBytes = 1

	functionAppContainer = "insights-logs-functionapplogs"
	flowEventContainer   = "insights-logs-networksecuritygroupflowevent"

	// MaxPayloadSize is the maximum byte size of the payload to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxPayloadSize = 4 * 1000000

	// MaxLogSize is the maximum byte size of a single log to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxLogSize = 1000000

	// MaxLogAge is the maximum age a log in the payload to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	MaxLogAge = 18 * time.Hour

	// bufferSize is the maximum number of logs per post to Logs API.
	// https://docs.datadoghq.com/api/latest/logs/
	bufferSize = 950
)

// ValidateDatadogLog checks if the log is valid to send to Datadog and returns the log size when it is.
func ValidateDatadogLog(log datadogV2.HTTPLogItem, logger *log.Entry) (int64, bool) {
	logBytes, err := log.MarshalJSON()
	if err != nil {
		logger.WithError(err).Warning("Failed to marshal log")
		return 0, false
	}

	var azLog *azureLog
	decoder := json.NewDecoder(bytes.NewReader([]byte(log.Message)))
	err = decoder.Decode(&azLog)
	if err != nil {
		logger.WithError(err).Warning("Failed to decode log as an azure log")
	}

	resourceId := "unknown"
	if azLog != nil {
		if r := azLog.ResourceId(); r != nil {
			resourceId = r.String()
		}
	}

	timeString, ok := log.AdditionalProperties["time"]
	if !ok {
		// log does not have a time field and cannot be validated
		logger.Warningf("Skipping log without a time field for resource %s", resourceId)
		return 0, false
	}
	parsedTime, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		// log has an invalid time field and cannot be validated
		logger.WithError(err).Warningf("Skipping log with an invalid time field for resource %s", resourceId)
		return 0, false
	}

	valid := validateLog(resourceId, int64(len(logBytes)), parsedTime, logger)
	return int64(len(logBytes)), valid
}

// validateLog checks if a log is valid to send to Datadog given a set of constraints.
func validateLog(resourceId string, byteSize int64, logTime time.Time, logger *log.Entry) bool {
	if byteSize > MaxLogSize {
		logger.Warningf("Skipping large log at %s from %s with a size of %d", logTime.Format(time.RFC3339), resourceId, byteSize)
		return false
	}
	if logTime.Before(time.Now().Add(-MaxLogAge)) {
		logger.Warningf("Skipping log older than 18 hours (at %s) for resource: %s", logTime.Format(time.RFC3339), resourceId)
		return false
	}
	return true
}

func sourceTag(resourceType string) string {
	val, ok := sourceTagMap[resourceType]
	if ok {
		return val
	}
	parts := strings.Split(strings.ToLower(resourceType), "/")
	tag := strings.Replace(parts[0], "microsoft.", "azure.", -1)
	sourceTagMap[resourceType] = tag
	return tag
}

func tagsFromResourceId(resourceId *arm.ResourceID) []string {
	if resourceId == nil {
		return []string{}
	}
	tags := []string{
		fmt.Sprintf("subscription_id:%s", resourceId.SubscriptionID),
		fmt.Sprintf("source:%s", sourceTag(resourceId.ResourceType.String())),
		fmt.Sprintf("resource_group:%s", resourceId.ResourceGroupName),
		fmt.Sprintf("resource_type:%s", resourceId.ResourceType.String()),
	}
	return tags
}

func astToAny(node any) (any, error) {
	switch v := node.(type) {
	case *ast.StringLiteral:
		return string(v.Value), nil
	case *ast.ObjectLiteral:
		valueMap, err := objectLiteralToMap(v)
		if err != nil {
			return nil, err
		}
		return valueMap, nil
	case *ast.NumberLiteral:
		return v.Value, nil
	case *ast.ArrayLiteral:
		items := make([]any, 0, len(v.Value))
		for _, item := range v.Value {
			parsedItem, err := astToAny(item)
			if err != nil {
				return nil, err
			}
			items = append(items, parsedItem)
		}
		return items, nil
	case *ast.BooleanLiteral:
		return v.Value, nil
	case *ast.NullLiteral:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected value type: %T", v)
	}
}

func objectLiteralToMap(objectLiteral *ast.ObjectLiteral) (map[string]any, error) {
	message := make(map[string]any)
	for idx := range objectLiteral.Value {
		propertyKeyed := objectLiteral.Value[idx].(*ast.PropertyKeyed)
		keyLiteral := propertyKeyed.Key.(*ast.StringLiteral)
		key := string(keyLiteral.Value)
		value, err := astToAny(propertyKeyed.Value)
		if err != nil {
			return nil, err
		}
		message[key] = value
	}
	return message, nil
}

func mapFromJSON(data []byte) (map[string]any, error) {
	javascriptExpression := fmt.Sprintf("a = %s;", data)
	program, err := parser.ParseFile(nil, "", javascriptExpression, 0)
	if err != nil {
		return nil, err
	}
	body := program.Body[0]
	statement := body.(*ast.ExpressionStatement)
	expression := statement.Expression.(*ast.AssignExpression)
	objectLiteral := expression.Right.(*ast.ObjectLiteral)
	return objectLiteralToMap(objectLiteral)
}

// BytesFromJavaScriptObject converts bytes representing a JavaScript object to bytes representing a JSON object.
func BytesFromJavaScriptObject(data []byte) ([]byte, error) {
	logMap, err := mapFromJSON(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(logMap)
}

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	additionalProperties := map[string]string{
		"time":            log.Time.Format(time.RFC3339),
		"level":           log.Level,
		"originContainer": log.Container,
		"originBlob":      log.Blob,
	}

	logItem := datadogV2.HTTPLogItem{
		Service:              pointer.Get(log.Service),
		Ddsource:             pointer.Get(log.Source),
		Ddtags:               pointer.Get(strings.Join(log.Tags, ",")),
		Message:              log.Content(),
		AdditionalProperties: additionalProperties,
	}
	return logItem
}

// DatadogLogsSubmitter wraps around the datadogV2.LogsApi struct.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type DatadogLogsSubmitter interface {
	SubmitLog(ctx context.Context, body []datadogV2.HTTPLogItem, o ...datadogV2.SubmitLogOptionalParameters) (interface{}, *http.Response, error)
}

// Client is a client for submitting logs to Datadog.
// It buffers logs and sends them in batches to the Datadog API.
// Client is not thread safe.
type Client struct {
	logsSubmitter DatadogLogsSubmitter
	logsBuffer    []datadogV2.HTTPLogItem
	currentSize   int64
	FailedLogs    []datadogV2.HTTPLogItem
}

// NewClient creates a new Client.
func NewClient(logsApi DatadogLogsSubmitter) *Client {
	return &Client{
		logsSubmitter: logsApi,
	}
}

// AddLog adds a log to the buffer for future submission.
func (c *Client) AddLog(ctx context.Context, logger *log.Entry, log *Log) (err error) {
	if !log.Validate(logger) {
		return nil
	}
	if c.shouldFlush(log) {
		err = c.Flush(ctx)
	}
	newLog := newHTTPLogItem(log)
	c.logsBuffer = append(c.logsBuffer, newLog)
	c.currentSize += log.ScrubbedLength()

	if err != nil {
		return err
	}
	return nil
}

// AddFormattedLog adds a datadog formatted log to the buffer for future submission.
func (c *Client) AddFormattedLog(ctx context.Context, logger *log.Entry, log datadogV2.HTTPLogItem) error {
	logBytes, valid := ValidateDatadogLog(log, logger)
	if !valid {
		return ErrInvalidLog
	}
	if c.shouldFlushBytes(logBytes) {
		if err := c.Flush(ctx); err != nil {
			return err
		}
	}

	c.logsBuffer = append(c.logsBuffer, log)
	c.currentSize += logBytes
	return nil
}

// Flush sends all buffered logs to the Datadog API.
func (c *Client) Flush(ctx context.Context) (err error) {
	if len(c.logsBuffer) > 0 {
		_, _, err = c.logsSubmitter.SubmitLog(ctx, c.logsBuffer)

		if err != nil {
			c.FailedLogs = append(c.FailedLogs, c.logsBuffer...)
		}

		c.logsBuffer = c.logsBuffer[:0]
		c.currentSize = 0
	}

	return err
}

// shouldFlush checks if adding the current log to the buffer would result in an invalid payload.
func (c *Client) shouldFlush(log *Log) bool {
	return c.shouldFlushBytes(log.ScrubbedLength())
}

// shouldFlushBytes checks if adding a log with a given size to the buffer would result in an invalid payload.
func (c *Client) shouldFlushBytes(bytes int64) bool {
	return len(c.logsBuffer)+1 >= bufferSize || c.currentSize+bytes >= MaxPayloadSize
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
