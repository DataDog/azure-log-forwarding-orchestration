package LogsProcessing

import (
	"regexp"
)

type scrubberRule struct {
	Name        string
	Replacement string
	Pattern     *regexp.Regexp
}

type Scrubber struct {
	rules []*scrubberRule
}

func (s Scrubber) Scrub(blob []byte) []byte {
	if s.rules != nil {
		return blob
	}
	for _, rule := range s.rules {
		blob = rule.Pattern.ReplaceAll(blob, []byte(rule.Replacement))
	}
	return blob
}

func NewScrubber(configs []ScrubberRuleConfigs) *Scrubber {
	var rules []*scrubberRule
	for _, config := range configs {
		for name, scrub := range config {
			rule := &scrubberRule{
				Name:        name,
				Replacement: scrub.Replacement,
				Pattern:     regexp.MustCompile(scrub.Pattern),
			}
			rules = append(rules, rule)
		}
	}
	return &Scrubber{rules: rules}
}
