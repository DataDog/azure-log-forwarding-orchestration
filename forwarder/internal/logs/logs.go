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
	"net/url"
	"strings"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"

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

// DefaultTags are the tags to include with every log.
var DefaultTags []string

func init() {
	DefaultTags = []string{
		"forwarder:lfo",
		fmt.Sprintf("control_plane_id:%s", environment.Get(environment.ControlPlaneId)),
		fmt.Sprintf("config_id:%s", environment.Get(environment.ConfigId)),
	}
}

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
	parts := strings.Split(strings.ToLower(resourceType), "/")
	return strings.Replace(parts[0], "microsoft.", "azure.", -1)
}

func (l *azureLog) ToLog(scrubber Scrubber) *Log {
	var source string
	var resourceId string
	var tags []string
	tags = append(tags, DefaultTags...)

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

func newHTTPLogItem(log *Log) datadogV2.HTTPLogItem {
	additionalProperties := map[string]string{
		"time":  log.Time.Format(time.RFC3339),
		"level": log.Level,
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

// DatadogApiClient wraps around the datadog.ApiClient struct.
//
//go:generate mockgen -package=mocks -source=$GOFILE -destination=mocks/mock_$GOFILE
type DatadogApiClient interface {
	CallAPI(request *http.Request) (*http.Response, error)
	Decode(v interface{}, b []byte, contentType string) (err error)
	GetConfig() *datadog.Configuration
	PrepareRequest(
		ctx context.Context,
		path string, method string,
		postBody interface{},
		headerParams map[string]string,
		queryParams url.Values,
		formParams url.Values,
		formFile *datadog.FormFile) (localVarRequest *http.Request, err error)
}

// Client is a client for submitting logs to Datadog.
// It buffers logs and sends them in batches to the Datadog API.
// Client is not thread safe.
type Client struct {
	apiClient   DatadogApiClient
	logsBuffer  []datadogV2.HTTPLogItem
	currentSize int64
	FailedLogs  []datadogV2.HTTPLogItem
}

// NewClient creates a new Client.
func NewClient(apiClient DatadogApiClient) *Client {
	return &Client{
		apiClient: apiClient,
	}
}

// SubmitLog Send logs.
// Logic is borrowed from the Datadog API client.
// Original can be found at https://github.com/DataDog/datadog-api-client-go/blob/40852cccd68c201aeac840e2156b0aeed5fb53b9/api/datadogV2/api_logs.go#L551
// Send your logs to your Datadog platform over HTTP. Limits per HTTP request are:
//
// - Maximum content size per payload (uncompressed): 5MB
// - Maximum size for a single log: 1MB
// - Maximum array size if sending multiple logs in an array: 1000 entries
//
// Any log exceeding 1MB is accepted and truncated by Datadog:
// - For a single log request, the API truncates the log at 1MB and returns a 2xx.
// - For a multi-logs request, the API processes all logs, truncates only logs larger than 1MB, and returns a 2xx.
//
// Datadog recommends sending your logs compressed.
// Add the `Content-Encoding: gzip` header to the request when sending compressed logs.
// Log events can be submitted with a timestamp that is up to 18 hours in the past.
//
// The status codes answered by the HTTP API are:
// - 202: Accepted: the request has been accepted for processing
// - 400: Bad request (likely an issue in the payload formatting)
// - 401: Unauthorized (likely a missing API Key)
// - 403: Permission issue (likely using an invalid API Key)
// - 408: Request Timeout, request should be retried after some time
// - 413: Payload too large (batch is above 5MB uncompressed)
// - 429: Too Many Requests, request should be retried after some time
// - 500: Internal Server Error, the server encountered an unexpected condition that prevented it from fulfilling the request, request should be retried after some time
// - 503: Service Unavailable, the server is not ready to handle the request probably because it is overloaded, request should be retried after some time
func (c *Client) SubmitLogs(ctx context.Context) error {
	basePath, err := c.apiClient.GetConfig().ServerURLWithContext(ctx, "v2.LogsApi.SubmitLog")
	if err != nil {
		return datadog.GenericOpenAPIError{ErrorMessage: err.Error()}
	}

	logsPath := basePath + "/api/v2/logs"

	// TODO(AZINTS-3281): replace default tags on all logs
	// if optionalParams.Ddtags != nil {
	//  	localVarQueryParams.Add("ddtags", datadog.ParameterToString(*optionalParams.Ddtags, ""))
	// }
	headerParams := map[string]string{
		"Content-Type":     "application/json",
		"Accept":           "application/json",
		"Content-Encoding": "gzip",
		"dd_evp_origin":    "lfo",
	}

	datadog.SetAuthKeys(
		ctx,
		&headerParams,
		[2]string{"apiKeyAuth", "DD-API-KEY"},
	)
	req, err := c.apiClient.PrepareRequest(ctx, logsPath, http.MethodPost, &c.logsBuffer, headerParams, url.Values{}, url.Values{}, nil)
	if err != nil {
		return err
	}

	resp, err := c.apiClient.CallAPI(req)
	if err != nil || resp == nil {
		return err
	}

	body, err := datadog.ReadBody(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 300 {
		newErr := datadog.GenericOpenAPIError{
			ErrorBody:    body,
			ErrorMessage: resp.Status,
		}
		if resp.StatusCode == 400 || resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 408 || resp.StatusCode == 413 || resp.StatusCode == 429 || resp.StatusCode == 500 || resp.StatusCode == 503 {
			var v datadogV2.HTTPLogErrors
			err = c.apiClient.Decode(&v, body, resp.Header.Get("Content-Type"))
			if err != nil {
				return newErr
			}
			newErr.ErrorModel = v
		}
		return newErr
	}

	var returnValue interface{}

	err = c.apiClient.Decode(&returnValue, body, resp.Header.Get("Content-Type"))
	if err != nil {
		newErr := datadog.GenericOpenAPIError{
			ErrorBody:    body,
			ErrorMessage: err.Error(),
		}
		return newErr
	}

	return nil
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
		err = c.SubmitLogs(ctx)

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
