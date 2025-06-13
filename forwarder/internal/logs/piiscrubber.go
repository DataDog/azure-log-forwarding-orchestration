// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package logs

import "regexp"

// Scrubber interface defines the Scrub behavior
type Scrubber interface {
	Scrub(logBytes []byte) []byte
}

// PiiScrubber holds the rule configs to execute for scrubbing
type PiiScrubber struct {
	ruleConfigs map[string]ScrubberRuleConfig
}

// ScrubberRuleConfig defines the regex pattern to search for and replacement string for a scrubber rule
type ScrubberRuleConfig struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
}

// NewPiiScrubber creates a new PiiScrubber with the given scrubberRuleConfigs
func NewPiiScrubber(scrubberRuleConfigs map[string]ScrubberRuleConfig) *PiiScrubber {
	return &PiiScrubber{ruleConfigs: scrubberRuleConfigs}
}

// Scrub matches regex patterns specified by the user's configs and replaces them with their corresponding replacement string
func (ps *PiiScrubber) Scrub(logBytes []byte) []byte {
	if len(ps.ruleConfigs) == 0 {
		return logBytes
	}

	content := string(logBytes)
	for _, scrubRule := range ps.ruleConfigs {
		regex, err := regexp.Compile(scrubRule.Pattern)
		if err != nil {
			continue
		}

		content = regex.ReplaceAllString(content, scrubRule.Replacement)
	}

	return []byte(content)
}
