CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.call_center_calls (
    id BIGSERIAL PRIMARY KEY,
    date TIMESTAMP,
    province TEXT,
    territoire TEXT,
    details TEXT,
    incident TEXT,
    categorie TEXT,
    genre TEXT,
    statut TEXT,
    record_count DOUBLE PRECISION,
    source_file TEXT,
    sheet_name TEXT,
    row_hash TEXT,
    imported_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.call_center_alerts (
    id BIGSERIAL PRIMARY KEY,
    date TIMESTAMP,
    location TEXT,
    indicator TEXT,
    value DOUBLE PRECISION,
    details TEXT,
    source_file TEXT,
    sheet_name TEXT,
    row_hash TEXT,
    imported_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.import_audit (
    id BIGSERIAL PRIMARY KEY,
    imported_at TIMESTAMPTZ DEFAULT NOW(),
    dataset_type TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_name_norm TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    sheet_name TEXT,
    date_min DATE,
    date_max DATE,
    total_rows INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    duplicate_rows INTEGER DEFAULT 0,
    missing_columns TEXT,
    missing_rows INTEGER DEFAULT 0,
    status TEXT NOT NULL,
    message TEXT
);

CREATE INDEX IF NOT EXISTS idx_call_center_calls_date ON public.call_center_calls(date);
CREATE INDEX IF NOT EXISTS idx_call_center_calls_province ON public.call_center_calls(province);
CREATE UNIQUE INDEX IF NOT EXISTS idx_call_center_calls_row_hash ON public.call_center_calls(row_hash);
CREATE INDEX IF NOT EXISTS idx_call_center_alerts_date ON public.call_center_alerts(date);
CREATE INDEX IF NOT EXISTS idx_call_center_alerts_location ON public.call_center_alerts(location);
CREATE UNIQUE INDEX IF NOT EXISTS idx_call_center_alerts_row_hash ON public.call_center_alerts(row_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_import_audit_dataset_file_hash ON public.import_audit(dataset_type, file_hash);
CREATE INDEX IF NOT EXISTS idx_import_audit_dataset_file_period ON public.import_audit(dataset_type, file_name_norm, date_min, date_max);
