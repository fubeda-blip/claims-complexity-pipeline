-- =========================================================
-- dbt Model: stg_claims.sql
-- =========================================================
-- Purpose:
-- 1. Read raw Claims CSV data from the Bronze Delta table
-- 2. Standardize column names and text values
-- 3. Cast raw string fields into proper data types
-- 4. Keep ingestion metadata for lineage/debugging
-- 5. Prepare clean Claims data for downstream Silver models
-- =========================================================


-- =========================================================
-- Source CTE:
-- Read raw claims data from Bronze
-- =========================================================

with source as (

    select
        *
    from {{ source('bronze', 'raw_claims') }}

),


-- =========================================================
-- Cleaned CTE:
-- Normalize and cast raw fields
-- =========================================================

cleaned as (

    select

        -- Unique claim identifier
        trim(cast(claim_id as string)) as claim_id,

        -- Convert date fields from string to date
        cast(date_of_loss as date) as date_of_loss,

        -- Standardize body part values to lowercase
        lower(trim(body_part)) as body_part,

        -- Convert litigation indicator to boolean
        cast(is_litigated as boolean) as is_litigated,

        -- Convert prior claim count to integer
        cast(number_of_priors as int) as number_of_priors,

        -- Preserve source file for lineage/debugging
        _source_file,

        -- Preserve ingestion timestamp
        _ingested_at,

        -- Preserve rescued data from Auto Loader, if any
        _rescued_data

    from source

),


-- =========================================================
-- Final CTE:
-- Apply basic row-level filters
-- =========================================================
-- Keep only records with a claim_id.
-- More strict validation is handled with dbt tests.
-- =========================================================

final as (

    select
        *
    from cleaned
    where claim_id is not null
      and claim_id <> ''

)


-- =========================================================
-- Final Output:
-- Cleaned and standardized claims staging model
-- =========================================================

select *
from final