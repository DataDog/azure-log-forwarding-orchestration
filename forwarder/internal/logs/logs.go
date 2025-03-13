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
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
)

const AzureService = "azure"

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
	content          *[]byte
	RawByteSize      int64
	ScrubbedByteSize int64
	Tags             []string
	Category         string
	ResourceId       string
	Service          string
	Source           string
	Time             time.Time
	Level            string
}

// Content converts the log content to a string.
func (l *Log) Content() string {
	return string(*l.content)
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

// ValidateDatadogLog checks if the log is valid to send to Datadog and returns the log size when it is.
func ValidateDatadogLog(log datadogV2.HTTPLogItem, logger *log.Entry) (int64, bool) {
	logBytes, err := log.MarshalJSON()
	if err != nil {
		logger.WithError(err).Warning("Failed to marshal log")
		return 0, false
	}

	var currLog *azureLog
	decoder := json.NewDecoder(bytes.NewReader([]byte(log.Message)))
	err = decoder.Decode(&currLog)
	if err != nil {
		logger.WithError(err).Warning("Failed to decode log to an azure log")
		return 0, false
	}

	resourceId := "unknown"
	if r := currLog.ResourceId(); r != nil {
		resourceId = r.String()
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

type azureLog struct {
	Raw             *[]byte
	ByteSize        int64
	Category        string `json:"category"`
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

func sourceTag(resourceType string) string {
	sourceTag := strings.ToLower(strings.Replace(resourceType, "/", ".", -1))
	return strings.Replace(sourceTag, "microsoft.", "azure.", -1)
}

func (l *azureLog) ToLog(scrubber Scrubber) *Log {
	var source string
	var resourceId string
	tags := []string{
		"forwarder:lfo",
		fmt.Sprintf("control_plane_id:%s", environment.Get(environment.CONTROL_PLANE_ID)),
		fmt.Sprintf("config_id:%s", environment.Get(environment.CONFIG_ID)),
	}

	// Try to add additional tags, source, and resource ID
	if parsedId := l.ResourceId(); parsedId != nil {
		source = sourceTag(parsedId.ResourceType.String())
		resourceId = parsedId.String()
		tags = append(tags,
			fmt.Sprintf("subscription_id:%s", parsedId.SubscriptionID),
			fmt.Sprintf("resource_group:%s", parsedId.ResourceGroupName),
			fmt.Sprintf("source:%s", source),
		)
	}

	if l.Level == "" {
		l.Level = "Informational"
	}

	scrubbedLog := scrubber.Scrub(l.Raw)
	scrubbedByteSize := len(*scrubbedLog) + newlineBytes // need to account for scrubed and raw log size so cursors remain accurate

	return &Log{
		content:          scrubbedLog,
		RawByteSize:      l.ByteSize,
		ScrubbedByteSize: int64(scrubbedByteSize),
		Category:         l.Category,
		ResourceId:       resourceId,
		Service:          AzureService,
		Source:           source,
		Time:             l.Time,
		Level:            l.Level,
		Tags:             tags,
	}
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
func NewLog(logBytes []byte, containerName, blobNameResourceId string, scrubber Scrubber) (*Log, error) {
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
		// log is not in JSON format, treat it as plaintext
		currLog = &azureLog{Time: time.Now()}
	}

	currLog.ByteSize = int64(logSize)
	currLog.Raw = &logBytes
	currLog.BlobResourceId = blobNameResourceId

	return currLog.ToLog(scrubber), nil
}

// bufferSize is the maximum number of logs per post to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const bufferSize = 950

// MaxPayloadSize is the maximum byte size of the payload to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const MaxPayloadSize = 4 * 1000000

// MaxLogSize is the maximum byte size of a single log to Logs API.
// https://docs.datadoghq.com/api/latest/logs/
const MaxLogSize = 1000000

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
		Service:              ptr(log.Service),
		Ddsource:             ptr(log.Source),
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
	logsBuffer    []datadogV2.HTTPLogItem
	currentSize   int64
	FailedLogs    []datadogV2.HTTPLogItem
	apiContext    context.Context
}

// NewClient creates a new Client.
func NewClient(logsApi DatadogLogsSubmitter) *Client {
	return &Client{
		logsSubmitter: logsApi,
	}
}

// NewClientWithCustomHeaders creates a new Client with support for custom headers.
// This function configures the Datadog API client to use our custom HTTP client.
func NewClientWithCustomHeaders(apiKey, appKey string) (*Client, error) {
	// Create a new Datadog API client configuration
	configuration := datadog.NewConfiguration()

	// Create a custom HTTP client that supports adding headers from the context
	configuration.HTTPClient = NewCustomHeadersClient(http.DefaultClient)

	// Create a new Datadog API client
	apiClient := datadog.NewAPIClient(configuration)

	// Create a context with API keys
	ctx := context.WithValue(
		context.Background(),
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: apiKey,
			},
			"appKeyAuth": {
				Key: appKey,
			},
		},
	)

	// Create a new logs API client with the context
	logsApi := datadogV2.NewLogsApi(apiClient)

	// Create a new client
	client := &Client{
		logsSubmitter: logsApi,
	}

	// Store the context for later use
	client.apiContext = ctx

	return client, nil
}

// SetCustomHeader sets a custom header on the underlying Datadog API client.
// This method requires the logsSubmitter to be of type *datadogV2.LogsApi.
func (c *Client) SetCustomHeader(key, value string) error {
	if logsApi, ok := c.logsSubmitter.(*datadogV2.LogsApi); ok {
		if logsApi.Client != nil && logsApi.Client.Cfg != nil {
			if logsApi.Client.Cfg.DefaultHeader == nil {
				logsApi.Client.Cfg.DefaultHeader = make(map[string]string)
			}
			logsApi.Client.Cfg.DefaultHeader[key] = value
			return nil
		}
		return fmt.Errorf("Datadog API client configuration is nil")
	}
	return fmt.Errorf("logsSubmitter is not of type *datadogV2.LogsApi")
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

// ErrInvalidLog is an error for when a log is invalid.
var ErrInvalidLog = errors.New("invalid log")

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
		// Use the API context if available, otherwise use the provided context
		requestCtx := ctx
		if c.apiContext != nil {
			requestCtx = c.apiContext
		}

		// Create optional parameters with Content-Encoding header for gzip compression
		optionalParams := datadogV2.SubmitLogOptionalParameters{
			ContentEncoding: datadogV2.CONTENTENCODING_GZIP.Ptr(),
		}

		// Submit the logs with gzip compression enabled
		_, _, err = c.logsSubmitter.SubmitLog(requestCtx, c.logsBuffer, optionalParams)

		if err != nil {
			c.FailedLogs = append(c.FailedLogs, c.logsBuffer...)
		}

		c.logsBuffer = c.logsBuffer[:0]
		c.currentSize = 0
	}

	return err
}

// customHeadersKey is the context key for custom headers
type customHeadersKey struct{}

// CustomHeadersTransport is an http.RoundTripper that adds custom headers from the context
type CustomHeadersTransport struct {
	Base http.RoundTripper
}

// RoundTrip implements the http.RoundTripper interface
func (t *CustomHeadersTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Get custom headers from the context
	if headers, ok := req.Context().Value(customHeadersKey{}).(map[string]string); ok {
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	}
	return t.Base.RoundTrip(req)
}

// NewCustomHeadersClient creates a new http.Client with a CustomHeadersTransport
func NewCustomHeadersClient(base *http.Client) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}
	return &http.Client{
		Transport: &CustomHeadersTransport{
			Base: base.Transport,
		},
		Timeout:       base.Timeout,
		Jar:           base.Jar,
		CheckRedirect: base.CheckRedirect,
	}
}

// WithCustomHeaders adds custom headers to a context
func WithCustomHeaders(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, customHeadersKey{}, headers)
}

// FlushWithHeaders sends all buffered logs to the Datadog API with custom headers.
// This method creates a new context with the custom headers and passes it to the Datadog API client.
func (c *Client) FlushWithHeaders(ctx context.Context, headers map[string]string) (err error) {
	if len(c.logsBuffer) > 0 {
		// Create optional parameters with Content-Encoding header for gzip compression
		optionalParams := datadogV2.SubmitLogOptionalParameters{
			ContentEncoding: datadogV2.CONTENTENCODING_GZIP.Ptr(),
		}

		// Create a new context with the custom headers and API keys
		requestCtx := ctx
		if c.apiContext != nil {
			requestCtx = c.apiContext
		}
		ctxWithHeaders := WithCustomHeaders(requestCtx, headers)

		// Submit the logs with gzip compression enabled and custom headers
		_, _, err = c.logsSubmitter.SubmitLog(ctxWithHeaders, c.logsBuffer, optionalParams)

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
func Parse(reader io.ReadCloser, containerName, blobNameResourceId string, piiScrubber Scrubber) iter.Seq2[*Log, error] {
	scanner := bufio.NewScanner(reader)

	// set buffer size so we can process logs bigger than 65kb
	buffer := make([]byte, initialBufferSize)
	scanner.Buffer(buffer, maxBufferSize)

	return func(yield func(*Log, error) bool) {
		for scanner.Scan() {
			currBytes := scanner.Bytes()
			currLog, err := NewLog(currBytes, containerName, blobNameResourceId, piiScrubber)
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

// AddCustomHeader adds a custom header to the next request.
// This is a convenience method that creates a new context with a single custom header.
func (c *Client) AddCustomHeader(ctx context.Context, key, value string) context.Context {
	return WithCustomHeaders(ctx, map[string]string{key: value})
}

// FlushWithHeader sends all buffered logs to the Datadog API with a single custom header.
// This is a convenience method that calls FlushWithHeaders with a single header.
func (c *Client) FlushWithHeader(ctx context.Context, key, value string) error {
	return c.FlushWithHeaders(ctx, map[string]string{key: value})
}
