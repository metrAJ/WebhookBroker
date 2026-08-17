CREATE TABLE events (
    index BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    issuer TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE webhooks (
    id SERIAL PRIMARY KEY,
    hook_url TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    last_processed_outbox_id BIGINT NOT NULL DEFAULT 0,
    current_retry INT NOT NULL DEFAULT 0,
    next_retry_time TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhooks_active ON webhooks(next_retry_time)
WHERE is_active = true;

CREATE TABLE outbox_deliveries (
    id BIGSERIAL PRIMARY KEY,
    event_index BIGINT NOT NULL REFERENCES events(index) ON DELETE CASCADE,
    webhook_id INT NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE
);

CREATE INDEX idx_outbox_delivery ON outbox_deliveries(id, webhook_id);