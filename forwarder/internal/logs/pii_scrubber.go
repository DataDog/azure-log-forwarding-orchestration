package logs

import "regexp"

type Scrubber interface {
	Scrub(logBytes *[]byte) *[]byte
}

type PiiScrubber struct {
	scrubberRuleConfigs map[string]ScrubberRuleConfig
}

type ScrubberRuleConfig struct {
	Pattern     string
	Replacement string
}

func NewPiiScrubber(scrubberRuleConfigs map[string]ScrubberRuleConfig) PiiScrubber {
	return PiiScrubber{scrubberRuleConfigs: scrubberRuleConfigs}
}

// Scrub will match regex patterns specified by the user and replace them with their specified replacement string
func (ps PiiScrubber) Scrub(logBytes *[]byte) *[]byte {
	if len(ps.scrubberRuleConfigs) == 0 {
		return logBytes
	}

	content := string(*logBytes)
	for _, scrubRule := range ps.scrubberRuleConfigs {
		regex, err := regexp.Compile(scrubRule.Pattern)
		if err != nil {
			// logger.Warningf("Failed to compile regex for pattern %s: %v", pattern, err)
			return logBytes
		}

		content = regex.ReplaceAllString(content, scrubRule.Replacement)
	}

	scrubbedBytes := []byte(content)
	return &scrubbedBytes
}
