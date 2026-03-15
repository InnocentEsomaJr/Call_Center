CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.call_center_records (
    id BIGSERIAL PRIMARY KEY,
    date TIMESTAMP,
    heure TEXT,
    numero TEXT,
    nom TEXT,
    prenom TEXT,
    province TEXT,
    territoire TEXT,
    item TEXT,
    details TEXT,
    details_appel TEXT,
    incident TEXT,
    type_pathologie TEXT,
    categorie TEXT,
    genre TEXT,
    statut TEXT,
    resolution TEXT,
    record_count DOUBLE PRECISION,
    source_file TEXT,
    sheet_name TEXT,
    source_kind TEXT,
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

CREATE TABLE IF NOT EXISTS public.dashboard_users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('administrateur', 'utilisateur')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    full_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_call_center_records_date ON public.call_center_records(date);
CREATE INDEX IF NOT EXISTS idx_call_center_records_province ON public.call_center_records(province);
CREATE INDEX IF NOT EXISTS idx_call_center_records_source_kind ON public.call_center_records(source_kind);
CREATE UNIQUE INDEX IF NOT EXISTS idx_call_center_records_row_hash ON public.call_center_records(row_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_import_audit_dataset_file_hash ON public.import_audit(dataset_type, file_hash);
CREATE INDEX IF NOT EXISTS idx_import_audit_dataset_file_period ON public.import_audit(dataset_type, file_name_norm, date_min, date_max);
CREATE INDEX IF NOT EXISTS idx_dashboard_users_role ON public.dashboard_users(role);
CREATE INDEX IF NOT EXISTS idx_dashboard_users_active ON public.dashboard_users(is_active);
