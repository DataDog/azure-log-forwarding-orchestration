// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs_test

import (
	"bufio"
	"bytes"
	_ "embed"
	"io"
	"testing"
	"time"

	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/storage"
)

// Embed all test fixtures for benchmarking
var (
	//go:embed fixtures/activedirectory/audit_logs.json
	benchADAuditLogs []byte

	//go:embed fixtures/activedirectory/managed_identity_sign_in_logs.json
	benchADManagedIdentityLogs []byte

	//go:embed fixtures/activedirectory/ms_graph_activity_logs.json
	benchADGraphActivityLogs []byte

	//go:embed fixtures/activedirectory/non_interactive_user_sign_in_logs.json
	benchADNonInteractiveLogs []byte

	//go:embed fixtures/activedirectory/risky_users_logs.json
	benchADRiskyUsersLogs []byte

	//go:embed fixtures/activedirectory/service_principal_sign_in_logs.json
	benchADServicePrincipalLogs []byte

	//go:embed fixtures/activedirectory/sign_in_logs.json
	benchADSignInLogs []byte

	//go:embed fixtures/activedirectory/user_risk_event_logs.json
	benchADUserRiskEventLogs []byte

	//go:embed fixtures/flowevent/networksecuritygroupflowevent_logs.json
	benchNSGFlowEventLogs []byte

	//go:embed fixtures/flowevent/vnetflowevent_logs.json
	benchVNetFlowEventLogs []byte

	//go:embed fixtures/aks_logs.json
	benchAKSLogs []byte

	//go:embed fixtures/function_app_logs.json
	benchFunctionAppLogs []byte

	//go:embed fixtures/function_app_logs_with_usa_short_timestamp.json
	benchFunctionAppUSALogs []byte

	//go:embed fixtures/workflowruntime_logs.json
	benchWorkflowRuntimeLogs []byte

	//go:embed fixtures/logs_with_level_as_int_or_string.json
	benchMixedLevelLogs []byte

	//go:embed fixtures/large_logs_buffer_test.json
	benchLargeLogsBuffer []byte
)

// Mock scrubber for benchmarking
type mockScrubber struct{}

func (m mockScrubber) Scrub(logBytes []byte) []byte { return logBytes }

// Helper function to run a benchmark with specific log data
func runParseBenchmark(b *testing.B, logData []byte, blobName string) {
	// Determine container name based on blob name
	containerName := "insights-logs-functionapplogs"
	if blobName == "networksecuritygroupflowevent_logs.json" {
		containerName = "insights-logs-networksecuritygroupflowevent"
	} else if blobName == "vnetflowevent_logs.json" {
		containerName = "insights-logs-flowlogflowevent"
	} else if blobName == "audit_logs.json" {
		containerName = "insights-logs-auditlogs"
	} else if blobName == "sign_in_logs.json" || blobName == "managed_identity_sign_in_logs.json" {
		containerName = "insights-logs-signinlogs"
	} else if blobName == "service_principal_sign_in_logs.json" {
		containerName = "insights-logs-serviceprincipalsigninlogs"
	} else if blobName == "non_interactive_user_sign_in_logs.json" {
		containerName = "insights-logs-noninteractiveusersigninlogs"
	} else if blobName == "ms_graph_activity_logs.json" {
		containerName = "insights-logs-microsoftgraphactivitylogs"
	} else if blobName == "risky_users_logs.json" {
		containerName = "insights-logs-riskyusers"
	} else if blobName == "user_risk_event_logs.json" {
		containerName = "insights-logs-userriskevents"
	}

	blob := storage.Blob{
		Container: storage.Container{
			Name: containerName,
		},
		Name:          blobName,
		ContentLength: int64(len(logData)),
		CreationTime:  time.Now(),
		LastModified:  time.Now(),
	}
	scrubber := mockScrubber{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := io.NopCloser(bytes.NewReader(logData))
		iter, _, err := logs.Parse(reader, blob, scrubber)
		if err != nil {
			b.Fatal(err)
		}

		// Consume all logs to measure full parsing performance
		count := 0
		for log := range iter {
			_ = log
			count++
		}

		if count == 0 {
			b.Fatal("No logs parsed")
		}
	}
}

// Benchmarks for Active Directory logs
func BenchmarkActiveDirectory_AuditLogs(b *testing.B) {
	runParseBenchmark(b, benchADAuditLogs, "audit_logs.json")
}

func BenchmarkActiveDirectory_ManagedIdentitySignIn(b *testing.B) {
	runParseBenchmark(b, benchADManagedIdentityLogs, "managed_identity_sign_in_logs.json")
}

func BenchmarkActiveDirectory_GraphActivity(b *testing.B) {
	runParseBenchmark(b, benchADGraphActivityLogs, "ms_graph_activity_logs.json")
}

func BenchmarkActiveDirectory_NonInteractiveSignIn(b *testing.B) {
	runParseBenchmark(b, benchADNonInteractiveLogs, "non_interactive_user_sign_in_logs.json")
}

func BenchmarkActiveDirectory_RiskyUsers(b *testing.B) {
	runParseBenchmark(b, benchADRiskyUsersLogs, "risky_users_logs.json")
}

func BenchmarkActiveDirectory_ServicePrincipalSignIn(b *testing.B) {
	runParseBenchmark(b, benchADServicePrincipalLogs, "service_principal_sign_in_logs.json")
}

func BenchmarkActiveDirectory_SignIn(b *testing.B) {
	runParseBenchmark(b, benchADSignInLogs, "sign_in_logs.json")
}

func BenchmarkActiveDirectory_UserRiskEvent(b *testing.B) {
	runParseBenchmark(b, benchADUserRiskEventLogs, "user_risk_event_logs.json")
}

// Benchmarks for Flow Event logs
func BenchmarkFlowEvent_NSG(b *testing.B) {
	runParseBenchmark(b, benchNSGFlowEventLogs, "networksecuritygroupflowevent_logs.json")
}

func BenchmarkFlowEvent_VNet(b *testing.B) {
	runParseBenchmark(b, benchVNetFlowEventLogs, "vnetflowevent_logs.json")
}

// Benchmarks for other log types
func BenchmarkAKS_Logs(b *testing.B) {
	runParseBenchmark(b, benchAKSLogs, "aks_logs.json")
}

func BenchmarkFunctionApp_Logs(b *testing.B) {
	runParseBenchmark(b, benchFunctionAppLogs, "function_app_logs.json")
}

func BenchmarkFunctionApp_USATimestamp(b *testing.B) {
	runParseBenchmark(b, benchFunctionAppUSALogs, "function_app_logs_with_usa_short_timestamp.json")
}

func BenchmarkWorkflowRuntime_Logs(b *testing.B) {
	runParseBenchmark(b, benchWorkflowRuntimeLogs, "workflowruntime_logs.json")
}

func BenchmarkMixedLevel_Logs(b *testing.B) {
	runParseBenchmark(b, benchMixedLevelLogs, "logs_with_level_as_int_or_string.json")
}

// Benchmark for large buffer management
func BenchmarkLargeBuffer_Logs(b *testing.B) {
	runParseBenchmark(b, benchLargeLogsBuffer, "large_logs_buffer_test.json")
}

// Composite benchmarks to test mixed workloads
func BenchmarkParse_MixedActiveDirectoryLogs(b *testing.B) {
	// Combine multiple AD log types to simulate mixed workload
	mixedLogs := append(benchADAuditLogs, benchADSignInLogs...)
	mixedLogs = append(mixedLogs, benchADServicePrincipalLogs...)
	runParseBenchmark(b, mixedLogs, "mixed_ad_logs.json")
}

func BenchmarkParse_MixedAllTypes(b *testing.B) {
	// Combine different log types to test parser selection overhead
	mixedLogs := append(benchAKSLogs, benchFunctionAppLogs...)
	mixedLogs = append(mixedLogs, benchNSGFlowEventLogs...)
	mixedLogs = append(mixedLogs, benchADSignInLogs...)
	runParseBenchmark(b, mixedLogs, "mixed_all_types.json")
}

// Parallel benchmarks to test concurrent parsing
func BenchmarkParse_Parallel(b *testing.B) {
	blob := storage.Blob{
		Container: storage.Container{
			Name: "insights-logs-networksecuritygroupflowevent",
		},
		Name:          "parallel_test.json",
		ContentLength: int64(len(benchNSGFlowEventLogs)),
		CreationTime:  time.Now(),
		LastModified:  time.Now(),
	}
	scrubber := mockScrubber{}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reader := io.NopCloser(bytes.NewReader(benchNSGFlowEventLogs))
			iter, _, err := logs.Parse(reader, blob, scrubber)
			if err != nil {
				b.Fatal(err)
			}

			count := 0
			for log := range iter {
				_ = log
				count++
			}

			if count == 0 {
				b.Fatal("No logs parsed")
			}
		}
	})
}

// Benchmark specific parser implementations directly
func BenchmarkFlowEventParser_Direct(b *testing.B) {
	blob := storage.Blob{
		Container: storage.Container{
			Name: "insights-logs-networksecuritygroupflowevent",
		},
		Name:          "networksecuritygroupflowevent_logs.json",
		ContentLength: int64(len(benchNSGFlowEventLogs)),
		CreationTime:  time.Now(),
		LastModified:  time.Now(),
	}
	scrubber := mockScrubber{}
	parser := logs.FlowEventParser{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(benchNSGFlowEventLogs)
		scanner := bufio.NewScanner(reader)
		scanner.Split(bufio.ScanLines)
		buffer := make([]byte, 1024*1024*5)
		scanner.Buffer(buffer, 1024*1024*1024)

		count := 0
		for log := range parser.Parse(scanner, blob, scrubber) {
			_ = log
			count++
		}

		if count == 0 {
			b.Fatal("No logs parsed")
		}
	}
}

func BenchmarkFunctionAppParser_Direct(b *testing.B) {
	blob := storage.Blob{
		Container: storage.Container{
			Name: "insights-logs-functionapplogs",
		},
		Name:          "function_app_logs.json",
		ContentLength: int64(len(benchFunctionAppLogs)),
		CreationTime:  time.Now(),
		LastModified:  time.Now(),
	}
	scrubber := mockScrubber{}
	parser := logs.FunctionAppParser{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(benchFunctionAppLogs)
		scanner := bufio.NewScanner(reader)
		scanner.Split(bufio.ScanLines)
		buffer := make([]byte, 1024*1024*5)
		scanner.Buffer(buffer, 1024*1024*1024)

		count := 0
		for log := range parser.Parse(scanner, blob, scrubber) {
			_ = log
			count++
		}

		if count == 0 {
			b.Fatal("No logs parsed")
		}
	}
}

func BenchmarkActiveDirectoryParser_Direct(b *testing.B) {
	blob := storage.Blob{
		Container: storage.Container{
			Name: "insights-logs-signinlogs",
		},
		Name:          "sign_in_logs.json",
		ContentLength: int64(len(benchADSignInLogs)),
		CreationTime:  time.Now(),
		LastModified:  time.Now(),
	}
	scrubber := mockScrubber{}
	parser := logs.ActiveDirectoryParser{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(benchADSignInLogs)
		scanner := bufio.NewScanner(reader)
		scanner.Split(bufio.ScanLines)
		buffer := make([]byte, 1024*1024*5)
		scanner.Buffer(buffer, 1024*1024*1024)

		count := 0
		for log := range parser.Parse(scanner, blob, scrubber) {
			_ = log
			count++
		}

		if count == 0 {
			b.Fatal("No logs parsed")
		}
	}
}