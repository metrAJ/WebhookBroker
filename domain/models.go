package domain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type Event struct {
	Index     int64
	EventID   string
	Issuer    string
	Data      string
	CreatedAt time.Time
}

type Webhook struct {
	ID                    int
	HookURL               string
	IsActive              bool
	Filters               FilterConfig
	LastProcessedOutboxID int64
	CurrentRetry          int
	NextRetryTime         *time.Time
	CreatedAt             time.Time
}

type OutboxDelivery struct {
	ID         int64
	EventIndex int64
	WebhookID  int
}

type DeliveryTask struct {
	WebhookID        int
	OutboxDeliveryID int64
	HookURL          string
	Payload          []byte
	CurrentRetry     int
}

type FilterParams struct {
	DivisibleBy *int    `json:"divisibleByN,omitempty"`
	Issuer      *string `json:"expectedIssuer,omitempty"`
	StartsWith  *string `json:"dataStartsWith,omitempty"`
}

type FilterConfig struct {
	Divisor    *int    `json:"div_n,omitempty"`
	Issuer     *string `json:"iss_match,omitempty"`
	StartsWith *string `json:"starts_with,omitempty"`
}

func (c FilterConfig) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *FilterConfig) Scan(value interface{}) error {
	if value == nil {
		*c = FilterConfig{}
		return nil
	}

	var b []byte

	switch v := value.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("unsupported type for FilterConfig: %T", value)
	}

	return json.Unmarshal(b, c)
}
