-- =========================================================
-- dbt Model: high_complexity_claims_daily.sql
-- =========================================================
-- Purpose:
-- 1. Read claim-level complexity scores from the Silver model
-- 2. Filter to only high-complexity claims
-- 3. Add reporting metadata
-- 4. Produce the final Gold business-facing table
-- =========================================================


-- =========================================================
-- dbt Configuration
-- =========================================================
-- Materialized as a table for simple reporting consumption.
-- This creates/refreshes the Gold table when dbt runs.
-- =========================================================

{{ config(
    materialized = 'table'
) }}


-- =========================================================
-- Source CTE:
-- Read reusable claim complexity model from Silver
-- =========================================================

with scored_claims as (

    select
        claim_id,
        date_of_loss,
        body_part,
        is_litigated,
        number_of_priors,
        has_surgery,
        has_opioid,
        complexity_score,
        complexity_band,
        _source_file,
        _ingested_at

    from {{ ref('int_claim_complexity_score') }}

),


-- =========================================================
-- High Complexity CTE:
-- Keep only high-complexity claims
-- =========================================================

high_complexity_claims as (

    select
        *
    from scored_claims
    where complexity_band = 'HIGH'

),


-- =========================================================
-- Final CTE:
-- Add business reporting fields
-- =========================================================

final as (

    select
        claim_id,
        date_of_loss,
        body_part,
        is_litigated,
        number_of_priors,
        has_surgery,
        has_opioid,
        complexity_score,
        complexity_band,

        -- Date the report/table was generated
        current_date() as report_date,

        -- Timestamp the record was processed into Gold
        current_timestamp() as processed_at,

        -- Preserve source lineage from upstream model
        _source_file,
        _ingested_at

    from high_complexity_claims

)


-- =========================================================
-- Final Output:
-- Gold business-ready table for high-complexity claims
-- =========================================================

select *
from final