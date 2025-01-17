package logs

import (
	// stdlib
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
)

// maxBufferSize is the maximum buffer to use for scanning logs.
// Logs greater than this buffer will be dropped by bufio.Scanner.
// The buffer is defaulted to the maximum value of an integer.
const maxBufferSize = math.MaxInt32

// initialBufferSize is the initial buffer size to use for scanning logs.
const initialBufferSize = 1024 * 1024 * 5

// newlineBytes is the number of bytes in a newline character in utf-8.
const newlineBytes = 1

const functionAppContainer = "insights-logs-functionapplogs"

// Log represents a log to send to Datadog.
type Log struct {
	content    *[]byte
	ByteSize   int64
	Tags       []string
	Category   string
	ResourceId string
	Time       time.Time
	Level      string
}

// Validate checks if the log is valid to send to Datadog.
func (l *Log) Validate(logger *log.Entry) bool {
	if l.ByteSize > MaxPayloadSize {
		logger.Warningf("Skipping large log from %s with a size of %d", l.ResourceId, l.Length())
		return false
	}
	if l.Time.Before(time.Now().Add(-MaxLogAge)) {
		logger.Warningf("Skipping log older than 18 hours for resource: %s", l.ResourceId)
		return false
	}
	return true
}

// Content converts the log content to a string.
func (l *Log) Content() string {
	return string(*l.content)
}

// Length returns the length of the log content.
func (l *Log) Length() int64 {
	return l.ByteSize
}

type azureLog struct {
	Raw             *[]byte
	ByteSize        int64
	Category        string    `json:"category"`
	ResourceIdLower string    `json:"resourceId,omitempty"`
	ResourceIdUpper string    `json:"ResourceId,omitempty"`
	Time            time.Time `json:"time"`
	Level           string    `json:"level,omitempty"`
}

func (l *azureLog) ResourceId() string {
	if l.ResourceIdLower != "" {
		return l.ResourceIdLower
	}
	return l.ResourceIdUpper
}

func (l *azureLog) ToLog() (*Log, error) {
	parsedId, err := arm.ParseResourceID(l.ResourceId())
	if err != nil {
		return nil, err
	}

	if l.Level == "" {
		l.Level = "Informational"
	}

	return &Log{
		content:    l.Raw,
		ByteSize:   l.ByteSize,
		Category:   l.Category,
		ResourceId: l.ResourceId(),
		Time:       l.Time,
		Level:      l.Level,
		Tags:       getTags(parsedId),
	}, nil
}

// ErrUnexpectedToken is an error for when an unexpected token is found in a log.
var ErrUnexpectedToken = errors.New("found unexpected token in log")

// ErrIncompleteLogFile is an error for when a log file is incomplete.
var ErrIncompleteLogFile = errors.New("received a partial log file")

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

// BytesFromJSON converts bytes representing a JavaScript object to bytes representing a JSON object.
func BytesFromJSON(data []byte) ([]byte, error) {
	logMap, err := mapFromJSON(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(logMap)
}

// NewLog creates a new Log from the given log bytes.
func NewLog(logBytes []byte, containerName string) (*Log, error) {
	var err error
	var currLog *azureLog

	logSize := len(logBytes) + newlineBytes

	if containerName == functionAppContainer {
		logBytes, err = BytesFromJSON(logBytes)
		if err != nil {
			if strings.Contains(err.Error(), "Unexpected token ;") {
				return nil, ErrUnexpectedToken
			}
			return nil, err
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(logBytes))
	err = decoder.Decode(&currLog)

	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrIncompleteLogFile
		}
		return nil, err
	}

	currLog.ByteSize = int64(logSize)
	currLog.Raw = &logBytes

	return currLog.ToLog()
}

func getTags(id *arm.ResourceID) []string {
	return []string{
		fmt.Sprintf("subscription_id:%s", id.SubscriptionID),
		fmt.Sprintf("resource_group:%s", id.ResourceGroupName),
		fmt.Sprintf("source:%s", strings.Replace(id.ResourceType.String(), "/", ".", -1)),
		fmt.Sprintf("forwarder:%s", "lfo"),
		fmt.Sprintf("control_plane_id:%s", environment.Get(environment.CONTROL_PLANE_ID)),
		fmt.Sprintf("config_id:%s", environment.Get(environment.CONFIG_ID)),
	}
}

// bufferSize is the maximum number of logs per post to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const bufferSize = 950

// MaxPayloadSize is the maximum byte size of the payload to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const MaxPayloadSize = 4 * 1000000

// MaxLogAge is the maximum age a log in the payload to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const MaxLogAge = 18 * time.Hour

func ptr[T any](v T) *T {
	return &v
}

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	additionalProperties := map[string]string{
		"time":  log.Time.Format(time.RFC3339),
		"level": log.Level,
	}

	logItem := datadogV2.HTTPLogItem{
		Ddsource:             ptr("azure"),
		Ddtags:               ptr(strings.Join(log.Tags, ",")),
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
	logsBuffer    []*Log
	currentSize   int64
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
	c.logsBuffer = append(c.logsBuffer, log)
	c.currentSize += log.Length()

	if err != nil {
		return err
	}
	return nil
}

// Flush sends all buffered logs to the Datadog API.
func (c *Client) Flush(ctx context.Context) (err error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "logs.Client.Flush")
	defer span.Finish(tracer.WithError(err))

	if len(c.logsBuffer) > 0 {
		logs := make([]datadogV2.HTTPLogItem, 0, len(c.logsBuffer))
		for _, currLog := range c.logsBuffer {
			logs = append(logs, newHTTPLogItem(currLog))
		}
		_, _, err = c.logsSubmitter.SubmitLog(ctx, logs)
		c.logsBuffer = c.logsBuffer[:0]
		c.currentSize = 0
	}

	return err
}

// shouldFlush checks if adding the current log to the buffer would result in an invalid payload.
func (c *Client) shouldFlush(log *Log) bool {
	return len(c.logsBuffer)+1 >= bufferSize || c.currentSize+log.Length() >= MaxPayloadSize
}

// ParseLogs reads logs from a reader and parses them into Log objects.
func ParseLogs(reader io.ReadCloser, containerName string) iter.Seq2[*Log, error] {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	return func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			currLog, err := NewLog(currBytes, containerName)
			if err != nil {
				if !yield(nil, err) {
					return
				}
			}
			if !yield(currLog, nil) {
				return
			}
		}
	}

}
