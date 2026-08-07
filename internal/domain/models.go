package domain

import (
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
