package api

import (
	"fmt"
	"net/url"
	"regexp"
	"webhookbroker/domain"

	"github.com/google/uuid"
)

type webhook struct {
	HookURL string
	Filters domain.FilterConfig
}

func (c webhook) Validate() error {
	u, err := url.ParseRequestURI(c.HookURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("api.go webhook.Validate(): invalid webhook URL: %s", c.HookURL)
	}

	if c.Filters.Divisor != nil && *c.Filters.Divisor == 0 {
		return fmt.Errorf("api.go webhook.Validate(): invalid filter (divisibleByN cannot be zero)")
	}

	if c.Filters.StartsWith != nil {
		match, _ := regexp.MatchString(`^[a-zA-Z0-9]+$`, *c.Filters.StartsWith)
		if !match {
			return fmt.Errorf("api.go webhook.Validate(): dataStartsWith must contain only alphanumeric characters")
		}
	}

	return nil
}

type event struct {
	EventID string
	Issuer  string
	Data    []byte
}

func (c event) Validate() error {
	if _, err := uuid.Parse(c.EventID); err != nil {
		return fmt.Errorf("invalid event id: must be a valid UUIDv4")
	}

	if len(c.Data) > 512*1024 {
		return fmt.Errorf("api.go event.Validate() Data is too large: %s", c.EventID)
	}

	return nil
}
