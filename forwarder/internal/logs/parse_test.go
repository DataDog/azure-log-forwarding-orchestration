// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs_test

import (
	// stdlib
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
)

func TestParseLogs(t *testing.T) {
	t.Parallel()

	t.Run("can parse aks logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/aks_logs.json", workingDir))
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, _ := logs.Parse(closer, newBlob(resourceId, "insights-logs-kube-audit"), MockScrubber(t, data))
		for parsedLog := range parsedLogsIter {
			currLog := parsedLog.ParsedLog
			require.NoError(t, parsedLog.Err)
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, got, 21)
	})

	t.Run("can parse function app logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/function_app_logs.json", workingDir))
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, _ := logs.Parse(closer, newBlob(resourceId, functionAppContainer), MockScrubber(t, data))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, got, 20)
	})

	t.Run("can parse workflow runtime logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/workflowruntime_logs.json", workingDir))
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, _ := logs.Parse(closer, newBlob(resourceId, worflowRuntimeContainer), MockScrubber(t, data))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.Equal(t, "WorkflowRuntime", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId) // resource id is overridden in the log
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, got, 7)
	})

	t.Run("can parse vnet flow logs", func(t *testing.T) {
		t.Parallel()
		// GIVEN
		workingDir, err := os.Getwd()
		require.NoError(t, err)

		data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/networksecuritygroupflowevent_logs.json", workingDir))
		require.NoError(t, err)

		reader := bytes.NewReader(data)
		closer := io.NopCloser(reader)

		var got int

		// WHEN
		parsedLogsIter, _ := logs.Parse(closer, newBlob(resourceId, "insights-logs-networksecuritygroupflowevent"), MockScrubber(t, data))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.Equal(t, "NetworkSecurityGroupFlowEvent", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId) // resource id is overridden in the log
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, 2, got)
	})
}

func TestParseActiveDirectoryLogs(t *testing.T) {
	t.Parallel()
	adResourceId := "/tenants/4d3bac44-0230-4732-9e70-cc00736f0a97/providers/Microsoft.aadiam"
	tests := map[string]struct {
		categoryName     string
		containerName    string
		testFileName     string
		expectedLogCount int
	}{
		"can parse audit logs": {
			categoryName:     "AuditLogs",
			containerName:    "insights-logs-auditlogs",
			testFileName:     "audit_logs.json",
			expectedLogCount: 22,
		},
		"can parse managed identity sign in logs": {
			categoryName:     "ManagedIdentitySignInLogs",
			containerName:    "insights-logs-managedidentitysigninlogs",
			testFileName:     "managed_identity_sign_in_logs.json",
			expectedLogCount: 24,
		},
		"can parse microsoft graph activity logs": {
			categoryName:     "MicrosoftGraphActivityLogs",
			containerName:    "insights-logs-microsoftgraphactivitylogs",
			testFileName:     "ms_graph_activity_logs.json",
			expectedLogCount: 25,
		},
		"can parse non interactive user sign in logs": {
			categoryName:     "NonInteractiveUserSignInLogs",
			containerName:    "insights-logs-noninteractiveusersigninlogs",
			testFileName:     "non_interactive_user_sign_in_logs.json",
			expectedLogCount: 14,
		},
		"can parse risky users logs": {
			categoryName:     "RiskyUsers",
			containerName:    "insights-logs-riskyusers",
			testFileName:     "risky_users_logs.json",
			expectedLogCount: 1,
		},
		"can parse service principal sign in logs": {
			categoryName:     "ServicePrincipalSignInLogs",
			containerName:    "insights-logs-serviceprincipalsigninlogs",
			testFileName:     "service_principal_sign_in_logs.json",
			expectedLogCount: 25,
		},
		"can parse sign in logs": {
			categoryName:     "SignInLogs",
			containerName:    "insights-logs-signinlogs",
			testFileName:     "sign_in_logs.json",
			expectedLogCount: 5,
		},
		"can parse user risk event logs": {
			categoryName:     "UserRiskEvents",
			containerName:    "insights-logs-userriskevents",
			testFileName:     "user_risk_event_logs.json",
			expectedLogCount: 1,
		},
	}

	workingDir, err := os.Getwd()
	require.NoError(t, err)

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// GIVEN
			data, err := os.ReadFile(fmt.Sprintf("%s/fixtures/activedirectory/%s", workingDir, testData.testFileName))
			require.NoError(t, err)

			reader := bytes.NewReader(data)
			closer := io.NopCloser(reader)

			var numLogsParsed int

			// WHEN
			parsedLogsIter, _ := logs.Parse(closer, newBlob(resourceId, testData.containerName), MockScrubber(t, data))
			for parsedLog := range parsedLogsIter {
				require.NoError(t, parsedLog.Err)
				require.Equal(t, testData.categoryName, parsedLog.ParsedLog.Category)
				require.Equal(t, testData.containerName, parsedLog.ParsedLog.Container)
				require.True(t, strings.EqualFold(adResourceId, parsedLog.ParsedLog.ResourceId))
				require.False(t, parsedLog.ParsedLog.Time.IsZero())
				numLogsParsed += 1
			}

			// THEN
			assert.Equal(t, testData.expectedLogCount, numLogsParsed)
		})
	}
}
