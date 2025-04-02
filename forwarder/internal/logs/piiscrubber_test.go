// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs_test

import (
	// stdlib
	"fmt"
	"strings"
	"testing"

	// 3p
	"github.com/stretchr/testify/assert"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/logs"
)

var piiLog = []byte("{ \"time\": \"2024-08-21T15:12:24Z\", \"ip\": \"100.1.34.201\", \"contact\": \"peanutbutter@jelly.com\", \"resourceId\": \"/SUBSCRIPTIONS/0B62A232-B8DB-4380-9DA6-640F7272ED6D/RESOURCEGROUPS/FORWARDER-INTEGRATION-TESTING/PROVIDERS/MICROSOFT.WEB/SITES/FORWARDERINTEGRATIONTESTING\", \"category\": \"FunctionAppLogs\", \"operationName\": \"Microsoft.Web/sites/functions/log\", \"level\": \"Informational\", \"location\": \"East US\", \"properties\": {'appName':'','roleInstance':'BD28A314-638598491096328853','message':'LoggerFilterOptions\\n{\\n  \\'MinLevel\\': \\'None\\',\\n  \\'Rules\\': [\\n    {\\n      \\'ProviderName\\': null,\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'None\\',\\n      \\'Filter\\': null\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Script.WebHost.Diagnostics.SystemLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': null,\\n      \\'Filter\\': \\'<AddFilter>b__0\\'\\n    },\\n    {\\n      \\'ProviderName\\': \\'Microsoft.Azure.WebJobs.Logging.ApplicationInsights.ApplicationInsightsLoggerProvider\\',\\n      \\'CategoryName\\': null,\\n      \\'LogLevel\\': \\'Trace\\',\\n      \\'Filter\\': null\\n    }\\n  ]\\n}','category':'Microsoft.Azure.WebJobs.Hosting.OptionsLoggingService','hostVersion':'4.34.2','hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48','level':'Information','levelId':2,'processId':60}}")

func TestPiiScrubber(t *testing.T) {
	t.Parallel()

	t.Run("no scrub occurs when pii pattern doesn't have match in log string", func(t *testing.T) {
		var scrubberRuleConfigs = map[string]logs.ScrubberRuleConfig{
			"no_matches": {
				Pattern:     `[0-9]*not in the log`,
				Replacement: "mango",
			},
			"still_no_matches": {
				Pattern:     `[a-zA-Z]*i am a banana`,
				Replacement: "apple",
			},
		}

		var piiScrubber = logs.NewPiiScrubber(scrubberRuleConfigs)
		scrubbed := piiScrubber.Scrub(piiLog)

		assert.Equal(t, string(piiLog), string(scrubbed))
	})

	t.Run("IP regex scrub should remmove IP addresses", func(t *testing.T) {
		piiStr := "100.1.34.201"
		replacement := "xxx.xxx.xxx.xxx"
		assert.Contains(t, string(piiLog), piiStr)

		var scrubberRuleConfigs = map[string]logs.ScrubberRuleConfig{
			"redact_ip": {
				Pattern:     `[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}`,
				Replacement: replacement,
			},
		}

		var piiScrubber = logs.NewPiiScrubber(scrubberRuleConfigs)
		scrubbed := piiScrubber.Scrub(piiLog)

		assert.Contains(t, string(scrubbed), fmt.Sprintf(`"ip": "%s"`, replacement))
		verifyScrubbedLog(t, string(piiLog), string(scrubbed), piiStr, replacement)
	})

	t.Run("execute all other regex if one is uncompilable", func(t *testing.T) {
		versionPii := `'hostVersion':'4.34.2'`
		idPii := `'hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48'`

		versionReplacement := "redacted_host_ver"
		idReplacement := "redacted_host_id"

		var scrubberRuleConfigs = map[string]logs.ScrubberRuleConfig{
			"bad_regex": {
				Pattern:     `(\d+)\1 hello world`,
				Replacement: "goodbye",
			},
			"redact_host_version": {
				Pattern:     `'hostVersion':'[0-9]*\.[0-9]*\.[0-9]*'`,
				Replacement: versionReplacement,
			},
			"redact_host_instance": {
				Pattern:     `'hostInstanceId':'[0-9a-zA-Z]*-[0-9a-zA-Z]*-[0-9a-zA-Z]*-[0-9a-zA-Z]*-[0-9a-zA-Z]*'`,
				Replacement: idReplacement,
			},
		}

		var piiScrubber = logs.NewPiiScrubber(scrubberRuleConfigs)
		scrubbed := piiScrubber.Scrub(piiLog)

		assert.NotContains(t, string(scrubbed), versionPii)
		assert.NotContains(t, string(scrubbed), idPii)
		assert.Contains(t, string(scrubbed), versionReplacement)
		assert.Contains(t, string(scrubbed), idReplacement)
		assert.Equal(t, 44, len(piiLog)-len(scrubbed))
	})

	t.Run("email regex scrub should remove emails", func(t *testing.T) {
		piiStr := "peanutbutter@jelly.com"
		replacement := "xxxxx@xxxxx.com"

		assert.Contains(t, string(piiLog), piiStr)

		var scrubberRuleConfigs = map[string]logs.ScrubberRuleConfig{
			"redact_email": {
				Pattern:     `[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+`,
				Replacement: replacement,
			},
		}

		var piiScrubber = logs.NewPiiScrubber(scrubberRuleConfigs)
		source := []byte(piiLog)
		scrubbed := piiScrubber.Scrub(source)

		assert.Contains(t, string(scrubbed), fmt.Sprintf(`"contact": "%s"`, replacement))
		verifyScrubbedLog(t, string(piiLog), string(scrubbed), piiStr, replacement)
	})

	t.Run("multiple string matches should replace all of them", func(t *testing.T) {
		piiStr := "Microsoft"
		replacement := "datadog"

		assert.Contains(t, string(piiLog), piiStr)
		assert.Equal(t, 5, strings.Count(string(piiLog), piiStr))

		var scrubberRuleConfigs = map[string]logs.ScrubberRuleConfig{
			"REDACT_MICROSOFT": {
				Pattern:     piiStr,
				Replacement: replacement,
			},
		}

		var piiScrubber = logs.NewPiiScrubber(scrubberRuleConfigs)
		scrubbed := piiScrubber.Scrub(piiLog)

		verifyScrubbedLog(t, string(piiLog), string(scrubbed), piiStr, replacement)
	})

	t.Run("multiple defined regex rules should all execute and replace", func(t *testing.T) {
		versionPii := `'hostVersion':'4.34.2'`
		idPii := `'hostInstanceId':'2800f488-b537-439f-9f79-88293ea88f48'`

		versionReplacement := "redacted_host_ver"
		idReplacement := "redacted_host_id"

		var scrubberRuleConfigs = map[string]logs.ScrubberRuleConfig{
			"redact_host_version": {
				Pattern:     `'hostVersion':'[0-9]*\.[0-9]*\.[0-9]*'`,
				Replacement: versionReplacement,
			},
			"redact_host_instance": {
				Pattern:     `'hostInstanceId':'[0-9a-zA-Z]*-[0-9a-zA-Z]*-[0-9a-zA-Z]*-[0-9a-zA-Z]*-[0-9a-zA-Z]*'`,
				Replacement: idReplacement,
			},
		}

		var piiScrubber = logs.NewPiiScrubber(scrubberRuleConfigs)
		scrubbed := piiScrubber.Scrub(piiLog)

		assert.NotContains(t, string(scrubbed), versionPii)
		assert.NotContains(t, string(scrubbed), idPii)
		assert.Contains(t, string(scrubbed), versionReplacement)
		assert.Contains(t, string(scrubbed), idReplacement)
		assert.Equal(t, 44, len(piiLog)-len(scrubbed))
	})
}

func verifyScrubbedLog(t *testing.T, original string, scrubbed string, piiStr string, replacement string) {
	assert.NotContains(t, scrubbed, piiStr, "scrubbed string has '%s' when it shouldn't", piiStr)
	assert.Contains(t, scrubbed, replacement, "scrubbed string does not have '%s' when it should", replacement)

	actualLogSizeDiff := len(original) - len(scrubbed)
	if actualLogSizeDiff < 0 {
		actualLogSizeDiff = -actualLogSizeDiff
	}

	strReplacementSizeDiff := len(piiStr) - len(replacement)
	if strReplacementSizeDiff < 0 {
		strReplacementSizeDiff = -strReplacementSizeDiff
	}

	// if there are multiple matches to the pii string, make sure we scrub all of them
	numReplacements := strings.Count(original, piiStr)
	expectedSizeDiff := numReplacements * strReplacementSizeDiff

	assert.Equal(t, actualLogSizeDiff, expectedSizeDiff, "log size difference: expected '%s' | actual '%s'", expectedSizeDiff, actualLogSizeDiff)
}
