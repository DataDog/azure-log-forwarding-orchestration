# Datadog APM Implementation for Azure Log Forwarding Orchestration

## Overview
Comprehensive Datadog APM tracing has been implemented across the Azure Log Forwarding Orchestration forwarder using dd-trace-go v1.69.1.

## Configuration

### Environment Variables
- `DD_APM_ENABLED` - Enable/disable APM (default: false)
- `DD_ENV` - Environment name
- `DD_SERVICE` - Service name (defaults to "azure-log-forwarder")
- `DD_VERSION` - Service version
- `DD_SITE` - Datadog site
- `RUN_ID` - Unique run identifier

### Enabling APM
Set `DD_APM_ENABLED=true` to enable tracing. The implementation is designed to have zero overhead when disabled.

## Instrumented Operations

### 1. Core Application Flow
- **forwarder.main** - Root span with error tracking
  - Tags: run_id
- **forwarder.run** - Main orchestration (45s timeout)
  - Tags: goroutine.count, version.tag
- **forwarder.fetchAndProcessLogs** - Core processing loop
  - Tags: logs.clients.count, version.tag

### 2. Azure Storage Operations
- **storage.Client.DownloadSegment**
  - Tags: azure.container.name, azure.blob.name, azure.blob.offset, azure.blob.content_length, azure.blob.segment_size
- **storage.Client.DownloadBlob**
  - Tags: azure.container.name, azure.blob.name, azure.blob.size
- **storage.Client.UploadBlob**
  - Tags: azure.container.name, azure.blob.name, azure.blob.size
- **storage.Client.ListBlobs** (via iterator)
  - Tags: azure.container.name

### 3. Log Processing Pipeline
- **logs.Parse** - Main parsing orchestrator with buffer pooling
- **logs.Client.AddLog** - Add log to buffer
- **logs.Client.AddRawLog** - Add raw Datadog log
- **logs.Client.Flush** - Send buffered logs to Datadog
  - Tags: logs.buffer.count, logs.buffer.size, logs.failed.count

### 4. Datadog API Operations
- **datadogV2.SubmitLog** - Submit logs to Datadog API (with GZIP compression)
  - Includes retry tracking with 90s timeout
  - Error tracking for failed submissions

### 5. State Management
- **cursor.Load** - Load cursors from blob storage
- **cursor.Save** - Save cursors to blob storage
- **deadletterqueue.Load** - Load DLQ from storage
- **deadletterqueue.Save** - Save DLQ to storage
- **metrics.writeMetrics** - Write metrics to storage

### 6. Concurrent Operations
All goroutine groups are tracked with proper context propagation:
- Download goroutines (limited concurrency)
- Log processing goroutines
- Volume tracking goroutine
- Bytes tracking goroutine

## Profiling
When APM is enabled, the following profilers are activated:
- CPU Profile
- Heap Profile
- Block Profile
- Mutex Profile
- Goroutine Profile

## Error Tracking
All spans properly capture and propagate errors using:
- `span.SetTag("error", true)`
- `span.SetTag("error.message", err.Error())`
- `span.Finish(tracer.WithError(err))`

## Performance Considerations
- Tracing is completely disabled when `DD_APM_ENABLED=false`
- All tracing operations are wrapped in conditional checks
- Span creation uses lazy evaluation for tags
- Context propagation maintains trace continuity across goroutines

## Testing the Implementation

### Local Testing
```bash
export DD_APM_ENABLED=true
export DD_ENV=development
export DD_SERVICE=azure-log-forwarder
export DD_VERSION=1.0.0
export DD_API_KEY=<your-api-key>
export DD_SITE=datadoghq.com

./forwarder
```

### Verifying Traces
1. Run the forwarder with APM enabled
2. Check Datadog APM UI at https://app.datadoghq.com/apm/traces
3. Filter by service: "azure-log-forwarder"
4. Verify trace flamegraphs show all instrumented operations

## Trace Visualization
The traces will appear as a flamegraph showing:
```
forwarder.main
└── forwarder.run
    └── forwarder.fetchAndProcessLogs
        ├── cursor.Load
        ├── storage.Client.ListBlobs (parallel)
        ├── storage.Client.DownloadSegment (parallel)
        ├── logs.Parse
        ├── logs.Client.AddLog
        ├── logs.Client.Flush
        │   └── datadogV2.SubmitLog
        ├── cursor.Save
        └── metrics.writeMetrics
```

## Future Enhancements
1. Add sampling configuration for high-volume environments
2. Implement custom metrics alongside traces
3. Add distributed tracing headers to correlate multiple forwarder instances
4. Create dashboards based on APM data
5. Set up alerts based on trace metrics

## Troubleshooting
- If traces don't appear, verify DD_API_KEY is set correctly
- Check logs for "APM tracer and profiler initialized" message
- Ensure DD_SITE matches your Datadog region
- Use `DD_TRACE_DEBUG=true` for detailed trace logging
