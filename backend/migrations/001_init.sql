CREATE TABLE IF NOT EXISTS cameras (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(128) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS variable_glossary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    variable VARCHAR(64) NOT NULL UNIQUE,
    description TEXT NULL,
    expected_cadence_seconds INTEGER NULL,
    is_image_like BOOLEAN NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS remote_date_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera VARCHAR(128) NOT NULL,
    variable VARCHAR(64) NOT NULL,
    timezone VARCHAR(8) NOT NULL DEFAULT 'local',
    date VARCHAR(10) NOT NULL,
    file_count INTEGER NULL,
    earliest_timestamp TIMESTAMP NULL,
    latest_timestamp TIMESTAMP NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_remote_date_cache UNIQUE (camera, variable, timezone, date)
);

CREATE TABLE IF NOT EXISTS file_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source VARCHAR(16) NOT NULL,
    camera VARCHAR(128) NOT NULL,
    variable VARCHAR(64) NOT NULL,
    date VARCHAR(10) NOT NULL,
    filename VARCHAR(512) NOT NULL,
    parsed_timestamp TIMESTAMP NULL,
    file_size INTEGER NULL,
    checksum VARCHAR(128) NULL,
    local_path VARCHAR(2048) NULL,
    ftp_path VARCHAR(2048) NULL,
    downloaded BOOLEAN NOT NULL DEFAULT 0,
    uploaded BOOLEAN NOT NULL DEFAULT 0,
    verified BOOLEAN NOT NULL DEFAULT 0,
    seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    downloaded_at TIMESTAMP NULL,
    uploaded_at TIMESTAMP NULL,
    verified_at TIMESTAMP NULL,
    metadata_json JSON NULL,
    CONSTRAINT uq_file_record_identity UNIQUE (source, camera, variable, date, filename)
);

CREATE TABLE IF NOT EXISTS local_date_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera VARCHAR(128) NOT NULL,
    variable VARCHAR(64) NOT NULL,
    date VARCHAR(10) NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    last_modified TIMESTAMP NULL,
    scanned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_local_date_inventory UNIQUE (camera, variable, date)
);

CREATE TABLE IF NOT EXISTS ftp_date_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    camera VARCHAR(128) NOT NULL,
    variable VARCHAR(64) NOT NULL,
    date VARCHAR(10) NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    scanned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    verified_at TIMESTAMP NULL,
    CONSTRAINT uq_ftp_date_inventory UNIQUE (camera, variable, date)
);

CREATE TABLE IF NOT EXISTS schedules (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT 1,
    job_kind VARCHAR(32) NOT NULL,
    cadence VARCHAR(32) NOT NULL,
    every_minutes INTEGER NULL,
    hour_of_day INTEGER NULL,
    minute_of_hour INTEGER NULL,
    day_of_week INTEGER NULL,
    day_of_month INTEGER NULL,
    params_json JSON NOT NULL DEFAULT '{}',
    next_run_at TIMESTAMP NULL,
    last_run_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(36) PRIMARY KEY,
    kind VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    params_json JSON NOT NULL DEFAULT '{}',
    progress_json JSON NULL,
    error_summary TEXT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    ended_at TIMESTAMP NULL,
    schedule_id VARCHAR(36) NULL REFERENCES schedules(id)
);

CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id VARCHAR(36) NOT NULL REFERENCES jobs(id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(16) NOT NULL,
    message TEXT NOT NULL,
    camera VARCHAR(128) NULL,
    variable VARCHAR(64) NULL,
    date VARCHAR(10) NULL,
    filename VARCHAR(512) NULL,
    reason VARCHAR(128) NULL,
    details_json JSON NULL
);

CREATE TABLE IF NOT EXISTS file_audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    camera VARCHAR(128) NOT NULL,
    variable VARCHAR(64) NOT NULL,
    date VARCHAR(10) NOT NULL,
    filename VARCHAR(512) NOT NULL,
    action VARCHAR(64) NOT NULL,
    source VARCHAR(16) NULL,
    reason VARCHAR(256) NULL,
    details_json JSON NULL,
    job_id VARCHAR(36) NULL REFERENCES jobs(id)
);

CREATE TABLE IF NOT EXISTS settings (
    key VARCHAR(128) PRIMARY KEY,
    value_json JSON NOT NULL DEFAULT '{}',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_cameras_name ON cameras(name);
CREATE INDEX IF NOT EXISTS ix_remote_date_cache_lookup ON remote_date_cache(camera, variable, timezone, date);
CREATE INDEX IF NOT EXISTS ix_file_records_lookup ON file_records(camera, variable, date, source);
CREATE INDEX IF NOT EXISTS ix_file_records_filename ON file_records(filename);
CREATE INDEX IF NOT EXISTS ix_local_date_inventory_lookup ON local_date_inventory(camera, variable, date);
CREATE INDEX IF NOT EXISTS ix_ftp_date_inventory_lookup ON ftp_date_inventory(camera, variable, date);
CREATE INDEX IF NOT EXISTS ix_jobs_created_at ON jobs(created_at);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS ix_jobs_kind ON jobs(kind);
CREATE INDEX IF NOT EXISTS ix_job_events_job_created ON job_events(job_id, created_at);
CREATE INDEX IF NOT EXISTS ix_file_audit_lookup ON file_audit_events(camera, variable, date, filename, created_at);
CREATE INDEX IF NOT EXISTS ix_file_audit_job ON file_audit_events(job_id, created_at);

