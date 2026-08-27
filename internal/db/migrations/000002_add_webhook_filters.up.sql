ALTER TABLE webhooks ADD COLUMN filters JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE worker_cursors (
    id VARCHAR(50) PRIMARY KEY,
    last_processed_index BIGINT NOT NULL DEFAULT 0
);

INSERT INTO worker_cursors (id, last_processed_index) VALUES ('main_dispatcher', 0);