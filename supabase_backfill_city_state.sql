-- ============================================================================
-- Backfill davinci_surgeons.city / state from the concatenated location string.
-- Safe to run multiple times.
--
-- What it does:
--   For any row where city or state is empty (or state is missing / invalid)
--   AND location contains a comma, parses location like "City, ST" or
--   "City, ST 12345" and populates the missing fields.
--
-- Why we only update NPI rows:
--   dedup_key is a STORED generated column that uses (lastname, firstname,
--   city, state) for no-NPI rows. Changing city/state on a no-NPI row would
--   regenerate dedup_key and could collide with an existing row's key.
--   The next scraper run will handle no-NPI rows correctly via the updated
--   Python parser, so it's safe to skip them here.
-- ============================================================================

WITH parsed AS (
    SELECT
        id,
        NULLIF(TRIM(SPLIT_PART(location, ',', 1)), '') AS parsed_city,
        UPPER(
            NULLIF(
                (regexp_match(SPLIT_PART(location, ',', 2), '\y([A-Za-z]{2})\y'))[1],
                ''
            )
        ) AS parsed_state
    FROM davinci_surgeons
    WHERE
        location IS NOT NULL
        AND location LIKE '%,%'
        AND npi IS NOT NULL AND npi <> ''
        AND (
            city  IS NULL OR city  = '' OR
            state IS NULL OR state = ''
        )
)
UPDATE davinci_surgeons ds
SET
    city  = COALESCE(NULLIF(ds.city, ''),  parsed.parsed_city,  ds.city),
    state = COALESCE(NULLIF(ds.state, ''), parsed.parsed_state, ds.state),
    updated_at = NOW()
FROM parsed
WHERE ds.id = parsed.id;

-- Quick verification: how many rows still have empty city or state?
SELECT
    COUNT(*)                                         AS total_rows,
    COUNT(*) FILTER (WHERE city  IS NULL OR city  = '') AS missing_city,
    COUNT(*) FILTER (WHERE state IS NULL OR state = '') AS missing_state,
    COUNT(*) FILTER (WHERE npi   IS NULL OR npi   = '') AS no_npi_rows
FROM davinci_surgeons;
