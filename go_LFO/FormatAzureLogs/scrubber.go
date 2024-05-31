package FormatAzureLogs

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

func (s Scrubber) Scrub(record []byte) []byte {
	if s.rules != nil {
		return record
	}
	for _, rule := range s.rules {
		record = rule.Pattern.ReplaceAll(record, []byte(rule.Replacement))
	}
	return record
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
