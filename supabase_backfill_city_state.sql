-- ============================================================================
-- Backfill davinci_surgeons.city / state from the concatenated location string.
-- Handles both "Dallas, TX" and "Dover, New Hampshire" formats.
-- Safe to run multiple times.
-- ============================================================================

-- Full state name → 2-letter code lookup.
CREATE OR REPLACE FUNCTION _state_name_to_code(name TEXT) RETURNS TEXT AS $$
    SELECT CASE LOWER(TRIM(name))
        WHEN 'alabama' THEN 'AL' WHEN 'alaska' THEN 'AK' WHEN 'arizona' THEN 'AZ'
        WHEN 'arkansas' THEN 'AR' WHEN 'california' THEN 'CA' WHEN 'colorado' THEN 'CO'
        WHEN 'connecticut' THEN 'CT' WHEN 'delaware' THEN 'DE'
        WHEN 'district of columbia' THEN 'DC' WHEN 'florida' THEN 'FL'
        WHEN 'georgia' THEN 'GA' WHEN 'hawaii' THEN 'HI' WHEN 'idaho' THEN 'ID'
        WHEN 'illinois' THEN 'IL' WHEN 'indiana' THEN 'IN' WHEN 'iowa' THEN 'IA'
        WHEN 'kansas' THEN 'KS' WHEN 'kentucky' THEN 'KY' WHEN 'louisiana' THEN 'LA'
        WHEN 'maine' THEN 'ME' WHEN 'maryland' THEN 'MD' WHEN 'massachusetts' THEN 'MA'
        WHEN 'michigan' THEN 'MI' WHEN 'minnesota' THEN 'MN' WHEN 'mississippi' THEN 'MS'
        WHEN 'missouri' THEN 'MO' WHEN 'montana' THEN 'MT' WHEN 'nebraska' THEN 'NE'
        WHEN 'nevada' THEN 'NV' WHEN 'new hampshire' THEN 'NH' WHEN 'new jersey' THEN 'NJ'
        WHEN 'new mexico' THEN 'NM' WHEN 'new york' THEN 'NY' WHEN 'north carolina' THEN 'NC'
        WHEN 'north dakota' THEN 'ND' WHEN 'ohio' THEN 'OH' WHEN 'oklahoma' THEN 'OK'
        WHEN 'oregon' THEN 'OR' WHEN 'pennsylvania' THEN 'PA' WHEN 'rhode island' THEN 'RI'
        WHEN 'south carolina' THEN 'SC' WHEN 'south dakota' THEN 'SD' WHEN 'tennessee' THEN 'TN'
        WHEN 'texas' THEN 'TX' WHEN 'utah' THEN 'UT' WHEN 'vermont' THEN 'VT'
        WHEN 'virginia' THEN 'VA' WHEN 'washington' THEN 'WA' WHEN 'west virginia' THEN 'WV'
        WHEN 'wisconsin' THEN 'WI' WHEN 'wyoming' THEN 'WY' WHEN 'puerto rico' THEN 'PR'
        WHEN 'guam' THEN 'GU' WHEN 'virgin islands' THEN 'VI'
        ELSE NULL
    END;
$$ LANGUAGE sql IMMUTABLE;


WITH parsed AS (
    SELECT
        id,
        NULLIF(TRIM(SPLIT_PART(location, ',', 1)), '') AS parsed_city,
        -- Take everything after the first comma; strip trailing zip (5 digits
        -- or ZIP+4); try full-state-name lookup first, else uppercase 2-letter code.
        COALESCE(
            _state_name_to_code(
                regexp_replace(TRIM(SPLIT_PART(location, ',', 2)), '\s+\d{5}(-\d{4})?\s*$', '')
            ),
            NULLIF(
                UPPER(TRIM(regexp_replace(SPLIT_PART(location, ',', 2), '\s+\d{5}(-\d{4})?\s*$', ''))),
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
WHERE ds.id = parsed.id
  -- Only accept parsed_state if it's a real 2-letter code (guards against
  -- accidentally storing a full state name in the state column).
  AND parsed.parsed_state IS NOT NULL
  AND LENGTH(parsed.parsed_state) = 2;

-- Verification
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(*) FILTER (WHERE city  IS NULL OR city  = '') AS missing_city,
    COUNT(*) FILTER (WHERE state IS NULL OR state = '') AS missing_state,
    COUNT(*) FILTER (WHERE LENGTH(state) <> 2 AND state <> '') AS bad_state_length,
    COUNT(*) FILTER (WHERE npi   IS NULL OR npi   = '') AS no_npi_rows
FROM davinci_surgeons;
