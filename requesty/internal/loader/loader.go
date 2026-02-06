package loader

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/requesty/internal/client"
	"github.com/DataDog/azure-log-forwarding-orchestration/requesty/internal/metrics"
	"golang.org/x/time/rate"
)

// Config holds the configuration for the loader
type Config struct {
	Client        *client.Client
	Collector     *metrics.Collector
	URL           string
	Duration      time.Duration
	RPS           int
	Workers       int
	WarmupPeriod  time.Duration
	RequestConfig RequestConfig
}

// RequestConfig holds configuration for request generation
type RequestConfig struct {
	Message     string
	Level       string
	Count       int
	VarietyMode bool
	Endpoint    string
	FunctionKey string
}

// Loader orchestrates the load test
type Loader struct {
	client    *client.Client
	collector *metrics.Collector
	config    Config
	limiter   *rate.Limiter
	wg        sync.WaitGroup
	progress  *metrics.ProgressBar
}

// New creates a new loader
func New(config Config) *Loader {
	// Create rate limiter
	limiter := rate.NewLimiter(rate.Limit(config.RPS), config.Workers)

	return &Loader{
		client:    config.Client,
		collector: config.Collector,
		config:    config,
		limiter:   limiter,
	}
}

// Run executes the load test
func (l *Loader) Run(ctx context.Context) error {
	// Start metrics collection
	l.collector.Start()
	defer l.collector.Stop()

	// Handle warmup period if configured
	if l.config.WarmupPeriod > 0 {
		if err := l.runWarmup(ctx); err != nil {
			return fmt.Errorf("warmup failed: %w", err)
		}
	}

	// Calculate total requests based on duration and RPS
	totalRequests := int(l.config.Duration.Seconds()) * l.config.RPS

	// Create progress bar
	l.progress = metrics.NewProgressBar(l.collector, totalRequests)

	// Create worker pool
	workChan := make(chan struct{}, totalRequests)
	resultChan := make(chan bool, totalRequests)

	// Fill work channel
	for i := 0; i < totalRequests; i++ {
		workChan <- struct{}{}
	}
	close(workChan)

	// Start workers
	workerCtx, cancel := context.WithTimeout(ctx, l.config.Duration)
	defer cancel()

	for i := 0; i < l.config.Workers; i++ {
		l.wg.Add(1)
		go l.worker(workerCtx, workChan, resultChan)
	}

	// Progress updater
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				l.progress.Update()
			case <-progressDone:
				return
			}
		}
	}()

	// Wait for workers to finish or timeout
	go func() {
		l.wg.Wait()
		close(resultChan)
	}()

	// Collect results
	select {
	case <-workerCtx.Done():
		// Test duration reached
	case <-ctx.Done():
		// Interrupted
		cancel()
	}

	// Stop progress updates
	close(progressDone)
	l.progress.Finish()

	// Wait for any remaining workers
	l.wg.Wait()

	return nil
}

// worker processes requests from the work channel
func (l *Loader) worker(ctx context.Context, workChan <-chan struct{}, resultChan chan<- bool) {
	defer l.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-workChan:
			if !ok {
				return
			}

			// Rate limiting
			if err := l.limiter.Wait(ctx); err != nil {
				return
			}

			// Make request
			success := l.makeRequest(ctx)
			resultChan <- success
		}
	}
}

// makeRequest executes a single request
func (l *Loader) makeRequest(ctx context.Context) bool {
	// Build request based on endpoint type
	var req *client.Request

	if l.config.RequestConfig.Endpoint == "customlog" {
		req = l.buildCustomLogRequest()
	} else {
		// Simple HTTP trigger
		req = &client.Request{
			Method: "GET",
			URL:    l.config.URL,
		}
	}

	// Execute request
	start := time.Now()
	resp, err := l.client.Do(ctx, req)
	duration := time.Since(start)

	// Record metrics
	if err != nil {
		l.collector.RecordRequest(false, 0, duration)
		return false
	}

	success := resp.IsSuccess()
	l.collector.RecordRequest(success, resp.StatusCode, duration)

	return success
}

// buildCustomLogRequest creates a request for the CustomLog endpoint
func (l *Loader) buildCustomLogRequest() *client.Request {
	config := l.config.RequestConfig

	// Generate variety if enabled
	message := config.Message
	level := config.Level
	count := config.Count

	if config.VarietyMode {
		message = l.generateVarietyMessage()
		level = l.generateVarietyLevel()
		count = l.generateVarietyCount()
	}

	queryParams := map[string]string{
		"message": message,
		"level":   level,
		"count":   fmt.Sprintf("%d", count),
	}

	// Add function key if provided
	if config.FunctionKey != "" {
		queryParams["code"] = config.FunctionKey
	}

	return &client.Request{
		Method:      "GET",
		URL:         l.config.URL,
		QueryParams: queryParams,
	}
}

// runWarmup performs a gradual warmup
func (l *Loader) runWarmup(ctx context.Context) error {
	fmt.Printf("\n🔥 Starting warmup period (%s)...\n", l.config.WarmupPeriod)

	steps := 5
	stepDuration := l.config.WarmupPeriod / time.Duration(steps)

	for i := 1; i <= steps; i++ {
		// Gradually increase RPS
		currentRPS := (l.config.RPS * i) / steps
		l.limiter.SetLimit(rate.Limit(currentRPS))

		fmt.Printf("  Warmup step %d/%d: %d RPS\n", i, steps, currentRPS)

		// Run for step duration
		timer := time.NewTimer(stepDuration)
		select {
		case <-timer.C:
			// Continue to next step
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	// Reset to full RPS
	l.limiter.SetLimit(rate.Limit(l.config.RPS))
	fmt.Println("  Warmup complete, starting main test...")

	return nil
}

// Variety generation functions for more realistic load

var (
	funMessages = []string{
		"Requesty was here!",
		"Testing in progress, please stand by...",
		"Is this thing on?",
		"Hello from the load tester side",
		"Knock knock... who's there? Logs!",
		"Loading... please wait... still loading...",
		"Test message #%d reporting for duty",
		"Azure Functions go brrrr",
		"I'm not a bug, I'm a feature request",
		"This is fine. Everything is fine.",
		"404: Clever message not found",
		"To log or not to log, that is the question",
		"May the logs be with you",
		"One does not simply test without logs",
		"Winter is coming... better check the logs",
		"I am Groot (translation: test log)",
		"The cake is a lie, but the logs are real",
		"All your logs are belong to us",
		"It's dangerous to go alone! Take this log!",
		"The princess is in another function app",
	}

	logLevels = []string{
		"debug",
		"info",
		"warning",
		"error",
		"critical",
	}

	// Weighted distribution for more realistic load
	levelWeights = []int{
		10, // debug - 10%
		60, // info - 60%
		20, // warning - 20%
		8,  // error - 8%
		2,  // critical - 2%
	}
)

// generateVarietyMessage returns a random fun message
func (l *Loader) generateVarietyMessage() string {
	msg := funMessages[rand.Intn(len(funMessages))]
	if rand.Float32() < 0.3 {
		// 30% chance to add timestamp
		msg = fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
	}
	if rand.Float32() < 0.2 {
		// 20% chance to add request ID
		msg = fmt.Sprintf("%s (ReqID: %d)", msg, rand.Intn(1000000))
	}
	return msg
}

// generateVarietyLevel returns a weighted random log level
func (l *Loader) generateVarietyLevel() string {
	totalWeight := 0
	for _, w := range levelWeights {
		totalWeight += w
	}

	r := rand.Intn(totalWeight)
	cumulative := 0

	for i, weight := range levelWeights {
		cumulative += weight
		if r < cumulative {
			return logLevels[i]
		}
	}

	return "info" // Default fallback
}

// generateVarietyCount returns a random count with bias towards smaller numbers
func (l *Loader) generateVarietyCount() int {
	r := rand.Float32()
	switch {
	case r < 0.7:
		return 1 // 70% single log
	case r < 0.9:
		return rand.Intn(5) + 1 // 20% 1-5 logs
	case r < 0.98:
		return rand.Intn(20) + 5 // 8% 5-25 logs
	default:
		return rand.Intn(50) + 25 // 2% 25-75 logs
	}
}

func init() {
	// Seed random number generator
	rand.Seed(time.Now().UnixNano())
}
