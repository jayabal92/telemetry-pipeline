-- Switch to telemetry DB
\c telemetry;

-- Telemetry Table
CREATE TABLE IF NOT EXISTS gpu_telemetry (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,         -- "timestamp"
    metric_name TEXT NOT NULL,       -- "metric_name"
    gpu_id INTEGER NOT NULL,            -- "gpu_id"
    device TEXT,                     -- "device"
    uuid TEXT,                       -- "uuid"
    model_name TEXT,                 -- "modelName"
    hostname TEXT,                   -- "Hostname"
    container TEXT,                  -- "container"
    pod TEXT,                        -- "pod"
    namespace TEXT,                  -- "namespace"
    metric_value DOUBLE PRECISION,   -- "value"
    labels_raw JSONB,                -- "labels_raw"
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Consumer Offset Table
CREATE TABLE IF NOT EXISTS consumer_offsets (
    group_id   TEXT NOT NULL,
    topic      TEXT NOT NULL,
    partition  INT  NOT NULL,
    committed_offset     BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (group_id, topic, partition)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS ix_telemetry_ts ON gpu_telemetry (ts DESC);
CREATE INDEX IF NOT EXISTS idx_gpu_ts ON gpu_telemetry (hostname, gpu_id, ts DESC);
CREATE INDEX IF NOT EXISTS ix_telemetry_metric_gpu ON gpu_telemetry (metric_name, gpu_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_labels_raw ON gpu_telemetry USING gin (labels_raw jsonb_path_ops);
