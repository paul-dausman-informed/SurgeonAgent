-- ============================================================================
-- da Vinci surgeon cache — Supabase schema
-- Run this once in the Supabase SQL Editor.
-- ============================================================================

CREATE TABLE IF NOT EXISTS davinci_surgeons (
    id                  BIGSERIAL PRIMARY KEY,
    npi                 TEXT,
    firstname           TEXT,
    lastname            TEXT NOT NULL,
    city                TEXT,
    state               TEXT,
    location            TEXT,
    profile_url         TEXT,
    procedure_count     TEXT,
    procedure_category  TEXT,
    specialties         JSONB DEFAULT '[]'::jsonb,
    procedures          JSONB DEFAULT '[]'::jsonb,
    hospitals           JSONB DEFAULT '[]'::jsonb,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Single dedup key: real NPI when present, else a normalized name+city
    -- composite. Lets us upsert every row via ON CONFLICT (dedup_key).
    dedup_key           TEXT GENERATED ALWAYS AS (
        COALESCE(
            NULLIF(npi, ''),
            'name:' || LOWER(COALESCE(lastname, '')) || '|'
                    || LOWER(COALESCE(firstname, '')) || '|'
                    || LOWER(COALESCE(city, '')) || '|'
                    || UPPER(COALESCE(state, ''))
        )
    ) STORED
);

-- Primary uniqueness on the generated dedup key (used for upserts).
CREATE UNIQUE INDEX IF NOT EXISTS davinci_surgeons_dedup_key
    ON davinci_surgeons (dedup_key);

-- Fast direct lookup by NPI (skip generated column indirection at read time).
CREATE INDEX IF NOT EXISTS davinci_surgeons_npi_idx
    ON davinci_surgeons (npi) WHERE npi IS NOT NULL AND npi <> '';

-- State index for fast filtering / debugging queries.
CREATE INDEX IF NOT EXISTS davinci_surgeons_state_idx
    ON davinci_surgeons (state);

-- Optional: view for at-a-glance manifest info.
CREATE OR REPLACE VIEW davinci_cache_manifest AS
SELECT
    COUNT(*)                                        AS total_surgeons,
    COUNT(*) FILTER (WHERE npi IS NOT NULL AND npi <> '') AS surgeons_with_npi,
    COUNT(*) FILTER (WHERE npi IS NULL OR npi = '') AS surgeons_without_npi,
    COUNT(DISTINCT state)                           AS states_covered,
    MAX(updated_at)                                 AS last_updated,
    MIN(updated_at)                                 AS oldest_row
FROM davinci_surgeons;

-- ============================================================================
-- Row-Level Security
--
-- Two access patterns:
--   1. The scraper uses SUPABASE_SERVICE_KEY, which bypasses RLS entirely.
--   2. The agent app uses SUPABASE_ANON_KEY and only reads.
--
-- Enable RLS and add a permissive SELECT policy for anon/authenticated so
-- reads work without giving out write access.
-- ============================================================================

ALTER TABLE davinci_surgeons ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "davinci_surgeons_read_all" ON davinci_surgeons;
CREATE POLICY "davinci_surgeons_read_all" ON davinci_surgeons
    FOR SELECT
    USING (true);
