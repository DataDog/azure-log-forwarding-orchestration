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

	// datadog
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// maxBufferSize is the maximum buffer to use for scanning logs.
// Logs greater than this buffer will be dropped by bufio.Scanner.
// The buffer is defaulted to the maximum value of an integer.
const maxBufferSize = math.MaxInt32

// initialBufferSize is the initial buffer size to use for scanning logs.
const initialBufferSize = 1024 * 1024 * 5

// newlineBytes is the number of bytes in a newline character in utf-8.
const newlineBytes = 2

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

// IsValid checks if the log is valid to send to Datadog.
func (l *Log) IsValid() bool {
	return l.ByteSize < MaxPayloadSize
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
	Raw         *[]byte
	ByteSize    int64
	Category    string    `json:"category"`
	ResourceIdA string    `json:"resourceId,omitempty"`
	ResourceIdB string    `json:"ResourceId,omitempty"`
	Time        time.Time `json:"time"`
	Level       string    `json:"level,omitempty"`
}

func (l *azureLog) ResourceId() string {
	if l.ResourceIdA != "" {
		return l.ResourceIdA
	}
	return l.ResourceIdB
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

// ErrIncompleteLog is an error for when a log is incomplete.
var ErrIncompleteLog = errors.New("received a partial log")

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
	program, err := parser.ParseFile(nil, "", "a = "+string(data)+";", 0)
	if err != nil {
		return nil, err
	}
	body := program.Body[0]
	statement := body.(*ast.ExpressionStatement)
	expression := statement.Expression.(*ast.AssignExpression)
	objectLiteral := expression.Right.(*ast.ObjectLiteral)
	return objectLiteralToMap(objectLiteral)
}

func BytesFromJSON(data []byte) ([]byte, error) {
	logMap, err := mapFromJSON(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(logMap)
}

// NewLog creates a new Log from the given log bytes.
func NewLog(blob storage.Blob, logBytes []byte) (*Log, error) {
	var err error
	var currLog *azureLog

	if blob.Container.Name == functionAppContainer {
		logBytes, err = BytesFromJSON(logBytes)
		if err != nil {
			return nil, err
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(logBytes))
	err = decoder.Decode(&currLog)

	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrIncompleteLog
		}
		return nil, err
	}

	currLog.ByteSize = int64(len(logBytes) + newlineBytes)
	currLog.Raw = &logBytes

	return currLog.ToLog()
}

func getTags(id *arm.ResourceID) []string {
	return []string{
		"subscription_id:" + id.SubscriptionID,
		"resource_group:" + id.ResourceGroupName,
		"source:" + strings.Replace(id.ResourceType.String(), "/", ".", -1),
		"forwarder:lfo",
	}
}

// TooLargeError represents an error for when a log is too large to send to Datadog.
type TooLargeError struct {
	log Log
}

// Error returns a string representation of the TooLargeError.
func (e TooLargeError) Error() string {
	return fmt.Sprintf("large log from %s with a size of %d", e.log.ResourceId, e.log.Length())
}

// bufferSize is the maximum number of logs per post to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const bufferSize = 950

// MaxPayloadSize is the maximum byte size of the payload to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const MaxPayloadSize = 4 * 1000000

func ptr[T any](v T) *T {
	return &v
}

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	logItem := datadogV2.HTTPLogItem{
		Ddsource: ptr("azure"),
		Ddtags:   ptr(strings.Join(log.Tags, ",")),
		Message:  log.Content(),
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
func (c *Client) AddLog(ctx context.Context, log *Log) (err error) {
	if !log.IsValid() {
		return TooLargeError{
			log: *log,
		}
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

func ParseLogs(blob storage.Blob, reader io.ReadCloser) iter.Seq2[*Log, error] {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	return func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			currLog, err := NewLog(blob, currBytes)
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
