// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package metrics

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Collector collects metrics from the load test
type Collector struct {
	mu sync.RWMutex

	// Counters (using atomic for lock-free updates)
	totalRequests      int64
	successfulRequests int64
	failedRequests     int64

	// Response times
	responseTimes []time.Duration

	// Error tracking
	errorsByCode map[int]int64

	// Timing
	startTime time.Time
	endTime   time.Time

	// Progress tracking
	progressCallback func(current, total int)
}

// Stats represents the collected statistics
type Stats struct {
	TotalRequests      int64         `json:"total_requests"`
	SuccessfulRequests int64         `json:"successful_requests"`
	FailedRequests     int64         `json:"failed_requests"`
	ErrorsByCode       map[int]int64 `json:"errors_by_code,omitempty"`
	MinLatency         time.Duration `json:"min_latency"`
	MaxLatency         time.Duration `json:"max_latency"`
	MeanLatency        time.Duration `json:"mean_latency"`
	P50                time.Duration `json:"p50"`
	P95                time.Duration `json:"p95"`
	P99                time.Duration `json:"p99"`
	RequestsPerSecond  float64       `json:"requests_per_second"`
	Duration           time.Duration `json:"duration"`
}

// NewCollector creates a new metrics collector
func NewCollector() *Collector {
	return &Collector{
		errorsByCode:  make(map[int]int64),
		responseTimes: make([]time.Duration, 0, 10000), // Pre-allocate for performance
		startTime:     time.Now(),
	}
}

// RecordRequest records a request result
func (c *Collector) RecordRequest(success bool, statusCode int, duration time.Duration) {
	// Atomic increments for counters
	atomic.AddInt64(&c.totalRequests, 1)

	if success {
		atomic.AddInt64(&c.successfulRequests, 1)
	} else {
		atomic.AddInt64(&c.failedRequests, 1)

		// Track error by status code (needs mutex)
		c.mu.Lock()
		c.errorsByCode[statusCode]++
		c.mu.Unlock()
	}

	// Store response time (needs mutex for slice append)
	c.mu.Lock()
	c.responseTimes = append(c.responseTimes, duration)
	c.mu.Unlock()

	// Call progress callback if set
	if c.progressCallback != nil {
		current := atomic.LoadInt64(&c.totalRequests)
		c.progressCallback(int(current), 0)
	}
}

// SetProgressCallback sets a callback for progress updates
func (c *Collector) SetProgressCallback(callback func(current, total int)) {
	c.progressCallback = callback
}

// Start marks the start of the test
func (c *Collector) Start() {
	c.startTime = time.Now()
}

// Stop marks the end of the test
func (c *Collector) Stop() {
	c.endTime = time.Now()
}

// GetStats returns the collected statistics
func (c *Collector) GetStats() *Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := &Stats{
		TotalRequests:      atomic.LoadInt64(&c.totalRequests),
		SuccessfulRequests: atomic.LoadInt64(&c.successfulRequests),
		FailedRequests:     atomic.LoadInt64(&c.failedRequests),
		ErrorsByCode:       make(map[int]int64),
	}

	// Copy error codes
	for code, count := range c.errorsByCode {
		stats.ErrorsByCode[code] = count
	}

	// Calculate latency percentiles if we have data
	if len(c.responseTimes) > 0 {
		// Make a copy and sort
		times := make([]time.Duration, len(c.responseTimes))
		copy(times, c.responseTimes)
		sort.Slice(times, func(i, j int) bool {
			return times[i] < times[j]
		})

		// Calculate percentiles
		stats.MinLatency = times[0]
		stats.MaxLatency = times[len(times)-1]
		stats.P50 = percentile(times, 0.50)
		stats.P95 = percentile(times, 0.95)
		stats.P99 = percentile(times, 0.99)

		// Calculate mean
		var sum time.Duration
		for _, t := range times {
			sum += t
		}
		stats.MeanLatency = sum / time.Duration(len(times))
	}

	// Calculate duration and RPS
	var duration time.Duration
	if c.endTime.IsZero() {
		duration = time.Since(c.startTime)
	} else {
		duration = c.endTime.Sub(c.startTime)
	}
	stats.Duration = duration

	if duration.Seconds() > 0 {
		stats.RequestsPerSecond = float64(stats.TotalRequests) / duration.Seconds()
	}

	return stats
}

// GetJSON returns the stats as JSON
func (c *Collector) GetJSON() string {
	stats := c.GetStats()

	// Convert durations to human-readable format for JSON
	type jsonStats struct {
		TotalRequests      int64         `json:"total_requests"`
		SuccessfulRequests int64         `json:"successful_requests"`
		FailedRequests     int64         `json:"failed_requests"`
		ErrorsByCode       map[int]int64 `json:"errors_by_code,omitempty"`
		MinLatency         string        `json:"min_latency"`
		MaxLatency         string        `json:"max_latency"`
		MeanLatency        string        `json:"mean_latency"`
		P50                string        `json:"p50"`
		P95                string        `json:"p95"`
		P99                string        `json:"p99"`
		RequestsPerSecond  float64       `json:"requests_per_second"`
		Duration           string        `json:"duration"`
	}

	js := jsonStats{
		TotalRequests:      stats.TotalRequests,
		SuccessfulRequests: stats.SuccessfulRequests,
		FailedRequests:     stats.FailedRequests,
		ErrorsByCode:       stats.ErrorsByCode,
		MinLatency:         formatDuration(stats.MinLatency),
		MaxLatency:         formatDuration(stats.MaxLatency),
		MeanLatency:        formatDuration(stats.MeanLatency),
		P50:                formatDuration(stats.P50),
		P95:                formatDuration(stats.P95),
		P99:                formatDuration(stats.P99),
		RequestsPerSecond:  stats.RequestsPerSecond,
		Duration:           formatDuration(stats.Duration),
	}

	data, _ := json.MarshalIndent(js, "", "  ")
	return string(data)
}

// GetCurrentCount returns the current request count
func (c *Collector) GetCurrentCount() int64 {
	return atomic.LoadInt64(&c.totalRequests)
}

// GetSuccessRate returns the current success rate
func (c *Collector) GetSuccessRate() float64 {
	total := atomic.LoadInt64(&c.totalRequests)
	if total == 0 {
		return 0
	}
	successful := atomic.LoadInt64(&c.successfulRequests)
	return float64(successful) / float64(total) * 100
}

// Reset resets all metrics
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	atomic.StoreInt64(&c.totalRequests, 0)
	atomic.StoreInt64(&c.successfulRequests, 0)
	atomic.StoreInt64(&c.failedRequests, 0)
	c.responseTimes = make([]time.Duration, 0, 10000)
	c.errorsByCode = make(map[int]int64)
	c.startTime = time.Now()
	c.endTime = time.Time{}
}

// percentile calculates the percentile value from a sorted slice
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int(float64(len(sorted)-1) * p)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

// formatDuration formats a duration for human readability
func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// ProgressBar provides a simple progress tracking interface
type ProgressBar struct {
	collector *Collector
	total     int
	lastPrint time.Time
	mu        sync.Mutex
}

// NewProgressBar creates a new progress bar
func NewProgressBar(collector *Collector, total int) *ProgressBar {
	return &ProgressBar{
		collector: collector,
		total:     total,
		lastPrint: time.Now(),
	}
}

// Update prints progress if enough time has passed
func (p *ProgressBar) Update() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Only update every 100ms to avoid spam
	if time.Since(p.lastPrint) < 100*time.Millisecond {
		return
	}

	current := p.collector.GetCurrentCount()
	successRate := p.collector.GetSuccessRate()

	// Clear line and print progress
	fmt.Printf("\r  Progress: %d/%d requests | Success rate: %.1f%%",
		current, p.total, successRate)

	p.lastPrint = time.Now()
}

// Finish completes the progress bar
func (p *ProgressBar) Finish() {
	fmt.Println() // New line after progress
}
