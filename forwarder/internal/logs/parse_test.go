// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs_test

import (
	// stdlib
	"bytes"
	"fmt"
	"io"
	"os"
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
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, "insights-logs-kube-audit"), MockScrubber(t, data))
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
		assert.Equal(t, len(data), *totalBytes)
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
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, functionAppContainer), MockScrubber(t, data))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.NotEqual(t, "", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId)
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		assert.Equal(t, 20, got)
		assert.Equal(t, len(data), *totalBytes)
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
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, worflowRuntimeContainer), MockScrubber(t, data))
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
		assert.Equal(t, len(data), *totalBytes)
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
		parsedLogsIter, totalBytes, _ := logs.Parse(closer, newBlob(resourceId, "insights-logs-networksecuritygroupflowevent"), MockScrubber(t, data))
		for parsedLog := range parsedLogsIter {
			require.NoError(t, parsedLog.Err)
			currLog := parsedLog.ParsedLog
			require.Equal(t, "NetworkSecurityGroupFlowEvent", currLog.Category)
			require.NotEqual(t, resourceId, currLog.ResourceId) // resource id is overridden in the log
			require.False(t, currLog.Time.IsZero())
			got += 1
		}

		// THEN
		// vnet flow logs have multiple logs per line
		assert.Equal(t, 2, got)
		assert.Equal(t, len(data), *totalBytes)

	})

}
