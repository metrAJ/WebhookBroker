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

func BuildChain(filters ...EventFilter) FilterChain {
	return filters
}
