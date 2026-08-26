package filters

import (
	"strings"
	"webhookbroker/domain"
)

type DivisibleFilter struct {
	Divisor int
}

func (f *DivisibleFilter) Matches(event domain.Event) bool {
	if f.Divisor == 0 {
		return false
	}

	return event.Index%int64(f.Divisor) == 0
}

type IssuerFilter struct {
	Expected string
}

func (f *IssuerFilter) Matches(event domain.Event) bool {
	return event.Issuer == f.Expected
}

type StartsWithFilter struct {
	Prefix string
}

func (f *StartsWithFilter) Matches(event domain.Event) bool {
	cleanData := strings.Trim(event.Data, `"`)
	return strings.HasPrefix(cleanData, f.Prefix)
}
