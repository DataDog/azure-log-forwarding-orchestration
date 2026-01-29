// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs_test

import (
	// stdlib
	"bytes"
	_ "embed"
	"io"
	"strings"
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
)

var (
	//go:embed fixtures/activedirectory/audit_logs.json
	adAuditLogData []byte

	//go:embed fixtures/activedirectory/managed_identity_sign_in_logs.json
	adManagedIdentitySignInLogData []byte

	//go:embed fixtures/activedirectory/ms_graph_activity_logs.json
	adMicrosoftGraphActivityLogData []byte

	//go:embed fixtures/activedirectory/non_interactive_user_sign_in_logs.json
	adNonInteractiveUserSignInLogData []byte

	//go:embed fixtures/activedirectory/risky_users_logs.json
	adRiskyUsersLogData []byte

	//go:embed fixtures/activedirectory/service_principal_sign_in_logs.json
	adServicePrincipalSignInLogData []byte

	//go:embed fixtures/activedirectory/sign_in_logs.json
	adSignInLogData []byte

	//go:embed fixtures/activedirectory/user_risk_event_logs.json
	adUserRiskEventLogData []byte

	//go:embed fixtures/aks_logs.json
	aksLogData []byte

	//go:embed fixtures/function_app_logs.json
	functionAppLogData []byte

	//go:embed fixtures/function_app_logs_with_usa_short_timestamp.json
	usaShortTimestampLogData []byte

	//go:embed fixtures/flowevent/networksecuritygroupflowevent_logs.json
	networkSecurityGroupFlowEventLogData []byte

	//go:embed fixtures/flowevent/vnetflowevent_logs.json
	vnetFlowLogData []byte

	//go:embed fixtures/workflowruntime_logs.json
	workflowRuntimeLogData []byte

	//go:embed fixtures/logs_with_level_as_int_or_string.json
	logsWithLevelAsIntOrStringData []byte

	//go:embed fixtures/large_logs_buffer_test.json
	largeLogsBufferTestData []byte
)

func TestParseLogs(t *testing.T) {
	t.Parallel()

	t.Run("can parse aks logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		reader := bytes.NewReader(aksLogData)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, "insights-logs-kube-audit"), MockScrubber(t, aksLogData))
		for parsedLog := range parsedLogsIter {
			currLog := parsedLog.ParsedLog
			require.NoError(t, parsedLog.Err)
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, 21, got)
		assert.Equal(t, len(aksLogData), *totalBytes)
	})

	t.Run("can parse function app logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		reader := bytes.NewReader(functionAppLogData)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob("bad-resource-id", functionAppContainer), MockScrubber(t, functionAppLogData))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			assert.Equal(t, "azure.web", parsedLog.ParsedLog.Source)
			assert.Contains(t, parsedLog.ParsedLog.Tags, "source:azure.web")
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, 20, got)
		assert.Equal(t, len(functionAppLogData), *totalBytes)
	})

	t.Run("can parse function app logs with short timestamps", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		reader := bytes.NewReader(usaShortTimestampLogData)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, functionAppContainer), MockScrubber(t, usaShortTimestampLogData))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, 5, got)
		assert.Equal(t, len(usaShortTimestampLogData), *totalBytes)
	})

	t.Run("can parse logs with level as integer", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		reader := bytes.NewReader(logsWithLevelAsIntOrStringData)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, worflowRuntimeContainer), MockScrubber(t, logsWithLevelAsIntOrStringData))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			got += 1
		}

		// THEN
		assert.Equal(t, 7, got)
		assert.Equal(t, len(logsWithLevelAsIntOrStringData), *totalBytes)
	})

	t.Run("can parse workflow runtime logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		reader := bytes.NewReader(workflowRuntimeLogData)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, worflowRuntimeContainer), MockScrubber(t, workflowRuntimeLogData))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.Equal(t, "WorkflowRuntime", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId) // resource id is overridden in the log
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, 7, got)
		assert.Equal(t, len(workflowRuntimeLogData), *totalBytes)
	})
}

func TestParseVnetFlowLogs(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		categoryName     string
		containerName    string
		resourceId       string
		logData          []byte
		expectedLogCount int
	}{
		"can parse vnet security group event logs": {
			categoryName:     "NetworkSecurityGroupFlowEvent",
			containerName:    logs.NetworkSecurityGroupFlowEventContainer,
			resourceId:       "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/CRONK-VM-2_GROUP/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/CRONK-VM-2-NSG",
			logData:          networkSecurityGroupFlowEventLogData,
			expectedLogCount: 2,
		},
		"can parse vnet flow event logs": {
			categoryName:     "FlowLogFlowEvent",
			containerName:    logs.VnetFlowEventContainer,
			resourceId:       "/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/NETWORKWATCHERRG/PROVIDERS/MICROSOFT.NETWORK/NETWORKWATCHERS/NETWORKWATCHER_EASTUS/FLOWLOGS/CRONK-VM-2-VNET-CRONK-VM-2_GROUP-FLOWLOG",
			logData:          vnetFlowLogData,
			expectedLogCount: 6,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// GIVEN
			reader := bytes.NewReader(testData.logData)
			closer := io.NopCloser(reader)

			var numLogsParsed int

			// WHEN
			parsedLogsIter, totalBytes, err := logs.Parse(closer, newBlob(testData.resourceId, testData.containerName), MockScrubber(t, testData.logData))
			require.NoError(t, err)

			for parsedLog := range parsedLogsIter {
				require.NoError(t, parsedLog.Err)
				currLog := parsedLog.ParsedLog
				require.Equal(t, testData.categoryName, currLog.Category)
				require.Equal(t, testData.containerName, currLog.Container)
				require.True(t, strings.EqualFold(testData.resourceId, currLog.ResourceId))
				require.False(t, currLog.Time.IsZero())
				numLogsParsed += 1
			}

			// THEN
			assert.Equal(t, len(testData.logData), *totalBytes)
			assert.Equal(t, testData.expectedLogCount, numLogsParsed)
		})
	}
}

func TestParseActiveDirectoryLogs(t *testing.T) {
	t.Parallel()
	adResourceId := "/tenants/4d3bac44-0230-4732-9e70-cc00736f0a97/providers/Microsoft.aadiam"
	tests := map[string]struct {
		categoryName     string
		containerName    string
		logData          []byte
		expectedLogCount int
	}{
		"can parse audit logs": {
			categoryName:     "AuditLogs",
			containerName:    "insights-logs-auditlogs",
			logData:          adAuditLogData,
			expectedLogCount: 22,
		},
		"can parse managed identity sign in logs": {
			categoryName:     "ManagedIdentitySignInLogs",
			containerName:    "insights-logs-managedidentitysigninlogs",
			logData:          adManagedIdentitySignInLogData,
			expectedLogCount: 24,
		},
		"can parse microsoft graph activity logs": {
			categoryName:     "MicrosoftGraphActivityLogs",
			containerName:    "insights-logs-microsoftgraphactivitylogs",
			logData:          adMicrosoftGraphActivityLogData,
			expectedLogCount: 25,
		},
		"can parse non interactive user sign in logs": {
			categoryName:     "NonInteractiveUserSignInLogs",
			containerName:    "insights-logs-noninteractiveusersigninlogs",
			logData:          adNonInteractiveUserSignInLogData,
			expectedLogCount: 14,
		},
		"can parse risky users logs": {
			categoryName:     "RiskyUsers",
			containerName:    "insights-logs-riskyusers",
			logData:          adRiskyUsersLogData,
			expectedLogCount: 1,
		},
		"can parse service principal sign in logs": {
			categoryName:     "ServicePrincipalSignInLogs",
			containerName:    "insights-logs-serviceprincipalsigninlogs",
			logData:          adServicePrincipalSignInLogData,
			expectedLogCount: 25,
		},
		"can parse sign in logs": {
			categoryName:     "SignInLogs",
			containerName:    "insights-logs-signinlogs",
			logData:          adSignInLogData,
			expectedLogCount: 5,
		},
		"can parse user risk event logs": {
			categoryName:     "UserRiskEvents",
			containerName:    "insights-logs-userriskevents",
			logData:          adUserRiskEventLogData,
			expectedLogCount: 1,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// GIVEN
			reader := bytes.NewReader(testData.logData)
			closer := io.NopCloser(reader)

			var numLogsParsed int

			// WHEN
			parsedLogsIter, totalBytes, err := logs.Parse(closer, newBlob(resourceId, testData.containerName), MockScrubber(t, testData.logData))
			require.NoError(t, err)

			for parsedLog := range parsedLogsIter {
				require.NoError(t, parsedLog.Err)
				require.Equal(t, testData.categoryName, parsedLog.ParsedLog.Category)
				require.Equal(t, testData.containerName, parsedLog.ParsedLog.Container)
				require.True(t, strings.EqualFold(adResourceId, parsedLog.ParsedLog.ResourceId))
				require.False(t, parsedLog.ParsedLog.Time.IsZero())
				numLogsParsed += 1
			}

			// THEN
			assert.Equal(t, len(testData.logData), *totalBytes)
			assert.Equal(t, testData.expectedLogCount, numLogsParsed)
		})
	}
}

// Regression [CLOUDS-7233]: Because of a shared reference to the internal scanner buffer,
// memory corruption would trigger under a particular set of circumstances
func TestParseLargeLogsBufferReuse(t *testing.T) {
	t.Parallel()

	// GIVEN: The set of circumstances triggering the bug:
	// - logs large enough to force buffer overwriting
	// - pii scrubber rules are not set
	reader := bytes.NewReader(largeLogsBufferTestData)
	closer := io.NopCloser(reader)
	blob := newBlob("/SUBSCRIPTIONS/TEST-SUB/RESOURCEGROUPS/TEST-RG/PROVIDERS/MICROSOFT.TEST/TEST", "insights-logs-test")
	scrubber := logs.NewPiiScrubber(nil)

	// WHEN: Parsed via logs.Parse()
	parsedLogsIter, _, err := logs.Parse(closer, blob, scrubber)
	require.NoError(t, err)

	var collectedLogs []*logs.Log
	for response := range parsedLogsIter {
		require.NoError(t, response.Err)
		collectedLogs = append(collectedLogs, response.ParsedLog)
	}

	// THEN: Make sure logs aren't corrupted/malformed
	firstLogContent := string(collectedLogs[0].Content)
	assert.Contains(t, firstLogContent, "FIRST_LOG_MARKER", "First log Content should contain FIRST_LOG_MARKER")
	assert.NotContains(t, firstLogContent, "SECOND_LOG_MARKER", "First log Content should NOT contain SECOND_LOG_MARKER (buffer reuse corruption)")

	secondLogContent := string(collectedLogs[1].Content)
	assert.Contains(t, secondLogContent, "SECOND_LOG_MARKER", "Second log Content should contain SECOND_LOG_MARKER")
}
