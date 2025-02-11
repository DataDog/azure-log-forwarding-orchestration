package logs

import "regexp"

type ScrubberRuleConfig struct {
	Pattern     string
	Replacement string
}

type PiiScrubber struct {
	scrubberRuleConfigs map[string]ScrubberRuleConfig
}

func NewPiiScrubber(scrubberRuleConfigs map[string]ScrubberRuleConfig) PiiScrubber {
	return PiiScrubber{scrubberRuleConfigs: scrubberRuleConfigs}
}

func (ps PiiScrubber) Scrub(logBytes []byte) []byte {
	content := string(logBytes)

	for _, scrubRule := range ps.scrubberRuleConfigs {
		regex, err := regexp.Compile(scrubRule.Pattern)
		if err != nil {
			// logger.Warningf("Failed to compile regex for pattern %s: %v", pattern, err)
			return logBytes
		}

		content = regex.ReplaceAllString(content, scrubRule.Replacement)
	}

	return []byte(content)
}
