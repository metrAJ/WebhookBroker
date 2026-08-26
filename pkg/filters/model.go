package filters

import (
	"webhookbroker/domain"
)

type EventFilter interface {
	Matches(event domain.Event) bool
}

type FilterChain []EventFilter

func (chain FilterChain) MatchesAll(event domain.Event) bool {
	for _, filter := range chain {
		if !filter.Matches(event) {
			return false
		}
	}
	return true
}

func BuildChain(config domain.FilterConfig) FilterChain {
	var chain FilterChain

	if config.Divisor != nil {
		chain = append(chain, &DivisibleFilter{
			Divisor: *config.Divisor,
		})
	}

	if config.Issuer != nil {
		chain = append(chain, &IssuerFilter{
			Expected: *config.Issuer,
		})
	}

	return chain
}
