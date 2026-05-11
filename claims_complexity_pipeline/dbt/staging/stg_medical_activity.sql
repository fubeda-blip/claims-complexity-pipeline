-- =========================================================
-- dbt Model: stg_medical_activity.sql
-- =========================================================
-- Purpose:
-- 1. Read raw Medical Activity JSON data from Bronze
-- 2. Flatten nested medical event records
-- 3. Standardize event types and text fields
-- 4. Cast event dates into proper date format
-- 5. Prepare clean medical activity data for scoring logic
-- =========================================================


-- =========================================================
-- Source CTE:
-- Read raw medical activity data from Bronze
-- =========================================================

with source as (

    select
        *
    from {{ source('bronze', 'raw_medical_activity') }}

),


-- =========================================================
-- Flattened CTE:
-- Explode nested medical_events array
-- =========================================================
-- This creates one row per claim per medical event.
-- Example:
-- Claim C123 with 3 medical events becomes 3 rows.
-- =========================================================

flattened as (

    select
        claim_id,

        -- Exploded individual medical event object
        event,

        -- Preserve source file for lineage/debugging
        _source_file,

        -- Preserve ingestion timestamp
        _ingested_at,

        -- Preserve rescued data from Auto Loader, if any
        _rescued_data

    from source
    lateral view explode(medical_events) exploded_events as event

),


-- =========================================================
-- Cleaned CTE:
-- Normalize and cast exploded event fields
-- =========================================================

cleaned as (

    select

        -- Unique claim identifier
        trim(cast(claim_id as string)) as claim_id,

        -- Standardized medical event type
        lower(trim(cast(event.type as string))) as event_type,

        -- Convert event date from string to date
        cast(event.date as date) as event_date,

        -- Preserve source file for lineage/debugging
        _source_file,

        -- Preserve ingestion timestamp
        _ingested_at,

        -- Preserve rescued data from Auto Loader, if any
        _rescued_data

    from flattened

),


-- =========================================================
-- Final CTE:
-- Apply basic row-level filters
-- =========================================================
-- Keep only records with claim_id and event_type.
-- More detailed validation is handled with dbt tests.
-- =========================================================

final as (

    select
        *
    from cleaned
    where claim_id is not null
      and claim_id <> ''
      and event_type is not null
      and event_type <> ''

)


-- =========================================================
-- Final Output:
-- Cleaned and flattened medical activity staging model
-- =========================================================

select *
from final