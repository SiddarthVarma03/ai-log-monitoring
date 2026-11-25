CREATE TABLE IF NOT EXISTS incidents (
    id BIGSERIAL PRIMARY KEY,
    block_id VARCHAR(100),
    event_id VARCHAR(50),
    log_level VARCHAR(20),
    component VARCHAR(255),
    events_seen INTEGER,
    anomaly_probability DOUBLE PRECISION,
    source_ip VARCHAR(45),
    destination_ip VARCHAR(45),
    summary TEXT,
    severity VARCHAR(20),
    likely_cause TEXT,
    recommended_action TEXT,
    raw_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
