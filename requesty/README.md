# Requesty 🚀

Your friendly load tester for Loggy - a silly-named but serious Golang load testing tool for Azure Function Apps.

```
██████╗ ███████╗ ██████╗ ██╗   ██╗███████╗███████╗████████╗██╗   ██╗
██╔══██╗██╔════╝██╔═══██╗██║   ██║██╔════╝██╔════╝╚══██╔══╝╚██╗ ██╔╝
██████╔╝█████╗  ██║   ██║██║   ██║█████╗  ███████╗   ██║    ╚████╔╝
██╔══██╗██╔══╝  ██║▄▄ ██║██║   ██║██╔══╝  ╚════██║   ██║     ╚██╔╝
██║  ██║███████╗╚██████╔╝╚██████╔╝███████╗███████║   ██║      ██║
╚═╝  ╚═╝╚══════╝ ╚═════╝  ╚═════╝ ╚══════╝╚══════╝   ╚═╝      ╚═╝
```

## Features

- **High-performance load testing** with configurable RPS and concurrent workers
- **Token bucket rate limiting** for precise request control
- **Real-time progress tracking** with colorful terminal output
- **Comprehensive metrics** including percentiles (P50, P95, P99)
- **Warmup period support** for gradual load ramping
- **Variety mode** with fun rotating messages and realistic log distributions
- **JSON output** for CI/CD integration
- **Graceful shutdown** with Ctrl+C handling

## Installation

### From source

```bash
# Clone the repository
git clone https://github.com/DataDog/azure-log-forwarding-orchestration.git
cd azure-log-forwarding-orchestration/requesty

# Build
make build

# Or directly with go
go build -o requesty cmd/requesty/main.go
```

### Quick build

```bash
cd requesty
go mod tidy
go build -o requesty cmd/requesty/main.go
```

## Usage

### Basic Load Test

```bash
# Test the CustomLog endpoint with default settings
requesty -url https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog -duration 30s -rps 10

# Test the HttpTrigger endpoint
requesty -url https://lfoms1829vm-loggy.azurewebsites.net/api/HttpTrigger -duration 60s -rps 50
```

### Advanced Usage

```bash
# High load test with custom parameters
requesty \
  -url https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog \
  -duration 120s \
  -rps 200 \
  -workers 50 \
  -message "Production load test" \
  -level error \
  -count 5

# Test with variety mode (random messages and levels)
requesty \
  -url https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog \
  -duration 60s \
  -rps 100 \
  -variety

# Gradual warmup before full load
requesty \
  -url https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog \
  -duration 60s \
  -rps 500 \
  -workers 100 \
  -warmup 30s

# Export results as JSON
requesty \
  -url https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog \
  -duration 30s \
  -rps 50 \
  -output json > results.json
```

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-url` | Target URL (required) | - |
| `-duration` | Test duration (e.g., 30s, 1m, 2h) | 30s |
| `-rps` | Requests per second | 10 |
| `-workers` | Number of concurrent workers | 10 |
| `-message` | Log message for CustomLog endpoint | "Test log from Requesty" |
| `-level` | Log level (debug/info/warning/error/critical) | info |
| `-count` | Number of logs per request (max 100) | 1 |
| `-output` | Output format (text/json) | text |
| `-warmup` | Warmup period before full load | 0 |
| `-variety` | Use variety of messages and levels | false |
| `-verbose` | Verbose output | false |

## Variety Mode

When `-variety` is enabled, Requesty generates diverse load patterns:

### Random Messages
- Fun messages like "Requesty was here!" and "May the logs be with you"
- 30% chance of timestamp prefix
- 20% chance of request ID suffix

### Weighted Log Levels
- Debug: 10%
- Info: 60%
- Warning: 20%
- Error: 8%
- Critical: 2%

### Log Count Distribution
- 70% single logs
- 20% batches of 1-5 logs
- 8% batches of 5-25 logs
- 2% batches of 25-75 logs

## Output Examples

### Text Output (Default)

```
📊 Test Configuration:
   Target URL:    https://lfoms1829vm-loggy.azurewebsites.net/api/CustomLog
   Endpoint:      customlog
   Duration:      30s
   RPS:           50
   Workers:       20
   Message:       Test log from Requesty
   Level:         info
   Count:         1

🚀 Starting load test...
  Progress: 1500/1500 requests | Success rate: 99.8%

✅ Load test completed!

📈 Test Results:

   Total Requests:     1500
   Successful:         1497 (99.8%)
   Failed:             3 (0.2%)

   Response Times:
     Min:              45.23ms
     P50 (Median):     120.45ms
     P95:              250.78ms
     P99:              380.12ms
     Max:              512.34ms
     Mean:             135.67ms

   Throughput:         49.90 req/s

   Errors by Status Code:
     503:              3

👍 Good run with minimal errors
```

### JSON Output

```json
{
  "total_requests": 1500,
  "successful_requests": 1497,
  "failed_requests": 3,
  "errors_by_code": {
    "503": 3
  },
  "min_latency": "45.23ms",
  "max_latency": "512.34ms",
  "mean_latency": "135.67ms",
  "p50": "120.45ms",
  "p95": "250.78ms",
  "p99": "380.12ms",
  "requests_per_second": 49.90,
  "duration": "30.07s"
}
```

## Architecture

Requesty is built with performance and accuracy in mind:

- **Worker Pool Pattern**: Fixed number of workers prevents goroutine explosion
- **Token Bucket Rate Limiting**: Precise RPS control using `golang.org/x/time/rate`
- **Lock-free Counters**: Atomic operations for metrics collection
- **Connection Pooling**: Reused HTTP connections for efficiency
- **Optimized Transport**: Configured for high-concurrency load testing

## Testing Strategy

1. **Start Small**: Begin with 10 RPS to verify connectivity
2. **Gradual Increase**: Use warmup periods for high loads
3. **Monitor Target**: Watch Azure Function App metrics during tests
4. **Verify Results**: Cross-check reported metrics with server-side logs

## Troubleshooting

### High Error Rates

If you see high error rates:
- Reduce RPS or increase workers
- Check if the function app is scaled properly
- Verify network connectivity
- Look for rate limiting or throttling

### Connection Errors

For connection-related issues:
- Increase the timeout (modify in code if needed)
- Check firewall rules
- Verify the URL is correct and accessible

### Memory Usage

For large tests:
- Response times are pre-allocated for 10,000 requests
- Consider shorter duration with higher RPS
- Monitor system resources during tests

## Development

### Project Structure

```
requesty/
├── cmd/
│   └── requesty/
│       └── main.go           # CLI entry point
├── internal/
│   ├── client/
│   │   └── http.go          # HTTP client wrapper
│   ├── loader/
│   │   └── loader.go        # Load test orchestrator
│   └── metrics/
│       └── collector.go     # Metrics collection
├── go.mod                   # Module definition
├── go.sum                   # Dependency checksums
├── Makefile                 # Build automation
└── README.md               # This file
```

### Building for Different Platforms

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o requesty-linux cmd/requesty/main.go

# macOS
GOOS=darwin GOARCH=amd64 go build -o requesty-darwin cmd/requesty/main.go

# Windows
GOOS=windows GOARCH=amd64 go build -o requesty.exe cmd/requesty/main.go
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run with race detection
go test -race ./...
```

## Contributing

1. Keep the silly name - it's part of the charm
2. Maintain the fun message variety
3. Ensure thread-safety for concurrent operations
4. Add tests for new features
5. Update this README

## License

Apache 2.0 - See parent repository LICENSE

## Fun Facts

- The name "Requesty" was chosen to be playfully silly
- The variety mode messages include gaming and pop culture references
- The ASCII art banner changes colors on each run
- There are exactly 20 different fun messages in variety mode
- The tool can theoretically generate up to 75 logs in a single request

---

Made with ❤️ and a sense of humor for testing Loggy
