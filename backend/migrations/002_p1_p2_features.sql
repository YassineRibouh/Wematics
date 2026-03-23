-- Additional performance indexes for large explorer and jobs queries.
CREATE INDEX IF NOT EXISTS ix_file_records_source_cam_var_date_ts
ON file_records(source, camera, variable, date, parsed_timestamp);

CREATE INDEX IF NOT EXISTS ix_file_records_source_date
ON file_records(source, date);

CREATE INDEX IF NOT EXISTS ix_file_records_source_filename
ON file_records(source, filename);

CREATE INDEX IF NOT EXISTS ix_jobs_status_kind_created
ON jobs(status, kind, created_at);

CREATE INDEX IF NOT EXISTS ix_schedules_enabled_next_run
ON schedules(enabled, next_run_at);

CREATE INDEX IF NOT EXISTS ix_job_events_created
ON job_events(created_at);

-- Idempotency support for enqueue dedupe.
ALTER TABLE jobs ADD COLUMN idempotency_key VARCHAR(128) NULL;
CREATE INDEX IF NOT EXISTS ix_jobs_idempotency_key ON jobs(idempotency_key);

-- CSV analysis cache by file hash and analysis options.
CREATE TABLE IF NOT EXISTS csv_analysis_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cache_key VARCHAR(256) NOT NULL UNIQUE,
    file_hash VARCHAR(64) NOT NULL,
    rows_limit INTEGER NOT NULL,
    time_column VARCHAR(128) NULL,
    value_column VARCHAR(128) NULL,
    result_json JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_csv_analysis_cache_file_hash
ON csv_analysis_cache(file_hash);
