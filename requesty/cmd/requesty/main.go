// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/requesty/internal/client"
	"github.com/DataDog/azure-log-forwarding-orchestration/requesty/internal/loader"
	"github.com/DataDog/azure-log-forwarding-orchestration/requesty/internal/metrics"
	"github.com/fatih/color"
)

const banner = `
██████╗ ███████╗ ██████╗ ██╗   ██╗███████╗███████╗████████╗██╗   ██╗
██╔══██╗██╔════╝██╔═══██╗██║   ██║██╔════╝██╔════╝╚══██╔══╝╚██╗ ██╔╝
██████╔╝█████╗  ██║   ██║██║   ██║█████╗  ███████╗   ██║    ╚████╔╝
██╔══██╗██╔══╝  ██║▄▄ ██║██║   ██║██╔══╝  ╚════██║   ██║     ╚██╔╝
██║  ██║███████╗╚██████╔╝╚██████╔╝███████╗███████║   ██║      ██║
╚═╝  ╚═╝╚══════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝   ╚═╝      ╚═╝
                    Your friendly load tester for Loggy              `

// Config holds the configuration for the load test
type Config struct {
	URL          string
	Duration     time.Duration
	RPS          int
	Workers      int
	Message      string
	Level        string
	Count        int
	OutputFormat string
	Verbose      bool
	WarmupPeriod time.Duration
	VarietyMode  bool
	Endpoint     string
	FunctionKey  string
}

func main() {
	// Parse command line flags
	config := parseFlags()

	// Print banner
	printBanner()

	// Validate configuration
	if err := validateConfig(config); err != nil {
		color.Red("❌ Configuration error: %v", err)
		os.Exit(1)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		color.Yellow("\n⚠️  Received interrupt signal, shutting down gracefully...")
		signal.Stop(sigChan)
		cancel()
	}()

	// Create HTTP client
	httpClient := client.New(client.Config{
		Timeout:     10 * time.Second,
		MaxIdleConn: config.Workers * 2,
		UserAgent:   "Requesty/1.0",
	})

	// Create metrics collector
	collector := metrics.NewCollector()

	// Create and configure loader
	ld := loader.New(loader.Config{
		Client:       httpClient,
		Collector:    collector,
		URL:          config.URL,
		Duration:     config.Duration,
		RPS:          config.RPS,
		Workers:      config.Workers,
		WarmupPeriod: config.WarmupPeriod,
		RequestConfig: loader.RequestConfig{
			Message:     config.Message,
			Level:       config.Level,
			Count:       config.Count,
			VarietyMode: config.VarietyMode,
			Endpoint:    config.Endpoint,
			FunctionKey: config.FunctionKey,
		},
	})

	// Print test configuration
	printTestConfig(config)

	// Run the load test
	color.Green("\n🚀 Starting load test...")
	if err := ld.Run(ctx); err != nil {
		color.Red("❌ Load test failed: %v", err)
		os.Exit(1)
	}

	// Print results
	color.Green("\n✅ Load test completed!")
	printResults(collector, config.OutputFormat)
}

func parseFlags() *Config {
	config := &Config{}

	// Define flags
	flag.StringVar(&config.URL, "url", "", "Target URL (required)")
	flag.DurationVar(&config.Duration, "duration", 30*time.Second, "Test duration")
	flag.IntVar(&config.RPS, "rps", 10, "Requests per second")
	flag.IntVar(&config.Workers, "workers", 10, "Number of concurrent workers")
	flag.StringVar(&config.Message, "message", "Test log from Requesty", "Log message for CustomLog endpoint")
	flag.StringVar(&config.Level, "level", "info", "Log level (debug/info/warning/error/critical)")
	flag.IntVar(&config.Count, "count", 1, "Number of logs per request (max 100)")
	flag.StringVar(&config.OutputFormat, "output", "text", "Output format (text/json)")
	flag.BoolVar(&config.Verbose, "verbose", false, "Verbose output")
	flag.DurationVar(&config.WarmupPeriod, "warmup", 0, "Warmup period before full load")
	flag.BoolVar(&config.VarietyMode, "variety", false, "Use variety of log messages and levels")
	flag.StringVar(&config.FunctionKey, "key", "", "Azure Function key for authentication")

	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Requesty - A load tester for Azure Functions\n\n")
		fmt.Fprintf(os.Stderr, "OPTIONS:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEXAMPLES:\n")
		fmt.Fprintf(os.Stderr, "  # Basic test against CustomLog endpoint\n")
		fmt.Fprintf(os.Stderr, "  requesty -url https://myapp.azurewebsites.net/api/CustomLog -duration 60s -rps 50\n\n")
		fmt.Fprintf(os.Stderr, "  # Test with custom message and error level\n")
		fmt.Fprintf(os.Stderr, "  requesty -url https://myapp.azurewebsites.net/api/CustomLog -message \"Error test\" -level error -count 5\n\n")
		fmt.Fprintf(os.Stderr, "  # High load test with warmup and variety mode\n")
		fmt.Fprintf(os.Stderr, "  requesty -url https://myapp.azurewebsites.net/api/CustomLog -rps 200 -workers 50 -warmup 10s -variety\n")
	}

	flag.Parse()

	// Determine endpoint type from URL
	if config.URL != "" {
		if strings.Contains(strings.ToLower(config.URL), "customlog") {
			config.Endpoint = "customlog"
		} else if strings.Contains(strings.ToLower(config.URL), "httptrigger") {
			config.Endpoint = "httptrigger"
		} else {
			config.Endpoint = "unknown"
		}
	}

	return config
}

func validateConfig(config *Config) error {
	if config.URL == "" {
		return fmt.Errorf("URL is required")
	}

	if config.RPS <= 0 {
		return fmt.Errorf("RPS must be greater than 0")
	}

	if config.Workers <= 0 {
		return fmt.Errorf("workers must be greater than 0")
	}

	if config.Count > 100 {
		return fmt.Errorf("count cannot exceed 100")
	}

	validLevels := map[string]bool{
		"debug":    true,
		"info":     true,
		"warning":  true,
		"error":    true,
		"critical": true,
	}

	if !validLevels[config.Level] {
		return fmt.Errorf("invalid log level: %s", config.Level)
	}

	return nil
}

func printBanner() {
	// Print colorful banner
	lines := strings.Split(banner, "\n")
	colors := []func(a ...interface{}) string{
		color.New(color.FgCyan).SprintFunc(),
		color.New(color.FgBlue).SprintFunc(),
		color.New(color.FgMagenta).SprintFunc(),
		color.New(color.FgCyan).SprintFunc(),
		color.New(color.FgBlue).SprintFunc(),
		color.New(color.FgMagenta).SprintFunc(),
		color.New(color.FgCyan).SprintFunc(),
		color.New(color.FgYellow).SprintFunc(),
	}

	for i, line := range lines {
		if i < len(colors) {
			fmt.Println(colors[i](line))
		} else {
			fmt.Println(line)
		}
	}
}

func printTestConfig(config *Config) {
	fmt.Println()
	color.Cyan("📊 Test Configuration:")
	fmt.Printf("   Target URL:    %s\n", config.URL)
	fmt.Printf("   Endpoint:      %s\n", config.Endpoint)
	fmt.Printf("   Duration:      %s\n", config.Duration)
	fmt.Printf("   RPS:           %d\n", config.RPS)
	fmt.Printf("   Workers:       %d\n", config.Workers)

	if config.Endpoint == "customlog" {
		fmt.Printf("   Message:       %s\n", config.Message)
		fmt.Printf("   Level:         %s\n", config.Level)
		fmt.Printf("   Count:         %d\n", config.Count)
		if config.VarietyMode {
			fmt.Printf("   Variety Mode:  Enabled\n")
		}
	}

	if config.WarmupPeriod > 0 {
		fmt.Printf("   Warmup:        %s\n", config.WarmupPeriod)
	}
}

func printResults(collector *metrics.Collector, format string) {
	if format == "json" {
		result := collector.GetJSON()
		fmt.Println(result)
		return
	}

	// Text format
	stats := collector.GetStats()

	if stats.TotalRequests == 0 {
		fmt.Println("\nNo requests completed.")
		return
	}

	fmt.Println()
	color.Cyan("📈 Test Results:")
	fmt.Println()

	// Summary
	fmt.Printf("   Total Requests:     %d\n", stats.TotalRequests)
	color.Green("   Successful:         %d (%.1f%%)\n",
		stats.SuccessfulRequests,
		float64(stats.SuccessfulRequests)/float64(stats.TotalRequests)*100)

	if stats.FailedRequests > 0 {
		color.Red("   Failed:             %d (%.1f%%)\n",
			stats.FailedRequests,
			float64(stats.FailedRequests)/float64(stats.TotalRequests)*100)
	}

	fmt.Println()

	// Performance metrics
	fmt.Println("   Response Times:")
	fmt.Printf("     Min:              %s\n", stats.MinLatency)
	fmt.Printf("     P50 (Median):     %s\n", stats.P50)
	fmt.Printf("     P95:              %s\n", stats.P95)
	fmt.Printf("     P99:              %s\n", stats.P99)
	fmt.Printf("     Max:              %s\n", stats.MaxLatency)
	fmt.Printf("     Mean:             %s\n", stats.MeanLatency)

	fmt.Println()

	// Throughput
	fmt.Printf("   Throughput:         %.2f req/s\n", stats.RequestsPerSecond)

	// Error breakdown if any
	if len(stats.ErrorsByCode) > 0 {
		fmt.Println()
		fmt.Println("   Errors by Status Code:")
		for code, count := range stats.ErrorsByCode {
			fmt.Printf("     %d:              %d\n", code, count)
		}
	}

	// Fun summary message
	fmt.Println()
	if stats.FailedRequests == 0 {
		color.Green("🎉 Perfect run! All requests succeeded!")
	} else if float64(stats.SuccessfulRequests)/float64(stats.TotalRequests) > 0.95 {
		color.Yellow("👍 Good run with minimal errors")
	} else if float64(stats.SuccessfulRequests)/float64(stats.TotalRequests) > 0.80 {
		color.Yellow("⚠️  Some issues detected, check your target service")
	} else {
		color.Red("❌ High error rate detected, target service may be struggling")
	}
}
