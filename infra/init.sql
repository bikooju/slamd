-- SLAMD Metrics DB Schema

CREATE TABLE IF NOT EXISTS slamd_run (
    job_id              TEXT PRIMARY KEY,
    job_type            TEXT,
    job_description     TEXT,
    job_class           TEXT,
    current_state       TEXT,
    start_time          TIMESTAMP,
    stop_time           TIMESTAMP,
    duration_seconds    INT,
    product_name        TEXT,
    num_clients         INT,
    threads_per_client  INT,
    thread_count        INT
);

CREATE TABLE IF NOT EXISTS slamd_op_summary (
    job_id              TEXT NOT NULL,
    op_type             TEXT NOT NULL,
    total_count         BIGINT,
    tps                 DOUBLE PRECISION,
    avg_latency_ms      DOUBLE PRECISION,
    std_dev             DOUBLE PRECISION,
    corr_coeff          DOUBLE PRECISION,
    success_rate        DOUBLE PRECISION,
    PRIMARY KEY (job_id, op_type),
    FOREIGN KEY (job_id) REFERENCES slamd_run(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS slamd_op_interval (
    job_id              TEXT NOT NULL,
    op_type             VARCHAR NOT NULL,
    collection_interval INT,
    counts              BIGINT[],
    ops_per_sec         DOUBLE PRECISION[],
    PRIMARY KEY (job_id, op_type),
    FOREIGN KEY (job_id) REFERENCES slamd_run(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS slamd_op_bucket (
    job_id              TEXT NOT NULL,
    op_type             TEXT NOT NULL,
    bucket_kind         TEXT NOT NULL,
    bucket_name         TEXT NOT NULL,
    count               BIGINT,
    percent             DOUBLE PRECISION,
    PRIMARY KEY (job_id, op_type, bucket_kind, bucket_name),
    FOREIGN KEY (job_id) REFERENCES slamd_run(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS slamd_job_params (
    job_id              TEXT PRIMARY KEY,
    params              JSONB NOT NULL,
    config_hash         TEXT,
    config_label        TEXT,
    FOREIGN KEY (job_id) REFERENCES slamd_run(job_id) ON DELETE CASCADE
);