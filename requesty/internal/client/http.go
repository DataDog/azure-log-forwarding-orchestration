// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config holds the configuration for the HTTP client
type Config struct {
	Timeout     time.Duration
	MaxIdleConn int
	UserAgent   string
}

// Client wraps the standard http.Client with additional functionality
type Client struct {
	httpClient *http.Client
	userAgent  string
}

// Request represents a request to be made
type Request struct {
	Method      string
	URL         string
	Headers     map[string]string
	Body        interface{}
	QueryParams map[string]string
}

// Response represents the response from a request
type Response struct {
	StatusCode int
	Body       []byte
	Headers    http.Header
	Duration   time.Duration
}

// New creates a new HTTP client with optimized settings for load testing
func New(config Config) *Client {
	transport := &http.Transport{
		MaxIdleConns:        config.MaxIdleConn,
		MaxIdleConnsPerHost: config.MaxIdleConn,
		MaxConnsPerHost:     config.MaxIdleConn,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		DisableKeepAlives:   false,
	}

	return &Client{
		httpClient: &http.Client{
			Timeout:   config.Timeout,
			Transport: transport,
		},
		userAgent: config.UserAgent,
	}
}

// Do executes an HTTP request and returns the response
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	// Build URL with query parameters
	reqURL, err := c.buildURL(req.URL, req.QueryParams)
	if err != nil {
		return nil, fmt.Errorf("failed to build URL: %w", err)
	}

	// Prepare request body
	var bodyReader io.Reader
	if req.Body != nil {
		bodyBytes, err := c.prepareBody(req.Body, req.Method)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	c.setHeaders(httpReq, req.Headers)

	// Execute request with timing
	start := time.Now()
	httpResp, err := c.httpClient.Do(httpReq)
	duration := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer httpResp.Body.Close()

	// Read response body
	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return &Response{
		StatusCode: httpResp.StatusCode,
		Body:       body,
		Headers:    httpResp.Header,
		Duration:   duration,
	}, nil
}

// Get performs a GET request
func (c *Client) Get(ctx context.Context, url string, queryParams map[string]string) (*Response, error) {
	return c.Do(ctx, &Request{
		Method:      "GET",
		URL:         url,
		QueryParams: queryParams,
	})
}

// Post performs a POST request
func (c *Client) Post(ctx context.Context, url string, body interface{}) (*Response, error) {
	return c.Do(ctx, &Request{
		Method: "POST",
		URL:    url,
		Body:   body,
	})
}

// buildURL constructs the full URL with query parameters
func (c *Client) buildURL(baseURL string, params map[string]string) (string, error) {
	if len(params) == 0 {
		return baseURL, nil
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	q := u.Query()
	for key, value := range params {
		q.Set(key, value)
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// prepareBody prepares the request body based on the content type
func (c *Client) prepareBody(body interface{}, method string) ([]byte, error) {
	if method == "GET" {
		return nil, nil
	}

	switch v := body.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case nil:
		return nil, nil
	default:
		// Assume JSON for other types
		return json.Marshal(body)
	}
}

// setHeaders sets the request headers
func (c *Client) setHeaders(req *http.Request, headers map[string]string) {
	// Set default headers
	req.Header.Set("User-Agent", c.userAgent)

	// Set content type for POST requests with body
	if req.Method == "POST" && req.Body != nil {
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}
	}

	// Set custom headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}
}

// IsSuccess checks if the response status code indicates success
func (r *Response) IsSuccess() bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

// String returns the response body as a string
func (r *Response) String() string {
	return string(r.Body)
}

// JSON unmarshals the response body into the provided interface
func (r *Response) JSON(v interface{}) error {
	return json.Unmarshal(r.Body, v)
}

// LoggyRequest represents a request specifically for the Loggy function app
type LoggyRequest struct {
	Message string `json:"message"`
	Level   string `json:"level"`
	Count   int    `json:"count"`
}

// BuildLoggyRequest creates a request for the CustomLog endpoint
func BuildLoggyRequest(baseURL string, message string, level string, count int) *Request {
	// Support both GET with query params and POST with JSON body
	if strings.Contains(strings.ToUpper(baseURL), "GET") {
		return &Request{
			Method: "GET",
			URL:    baseURL,
			QueryParams: map[string]string{
				"message": message,
				"level":   level,
				"count":   fmt.Sprintf("%d", count),
			},
		}
	}

	// Default to POST
	return &Request{
		Method: "POST",
		URL:    baseURL,
		Body: LoggyRequest{
			Message: message,
			Level:   level,
			Count:   count,
		},
	}
}
