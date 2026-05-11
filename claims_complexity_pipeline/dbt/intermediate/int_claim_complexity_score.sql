-- =========================================================
-- dbt Model: int_claim_complexity_score.sql
-- =========================================================
-- Purpose:
-- 1. Read cleaned claims data from stg_claims
-- 2. Read cleaned medical activity data from stg_medical_activity
-- 3. Aggregate medical activity into claim-level flags
-- 4. Join claims with medical indicators
-- 5. Apply claim complexity scoring logic
-- 6. Produce a reusable Silver model for Gold reporting
-- =========================================================


-- =========================================================
-- Claims CTE:
-- Read standardized claims data
-- =========================================================

with claims as (

    select
        claim_id,
        date_of_loss,
        body_part,
        is_litigated,
        number_of_priors,
        _source_file,
        _ingested_at

    from {{ ref('stg_claims') }}

),


-- =========================================================
-- Medical Flags CTE:
-- Convert many medical activity rows into one row per claim
-- =========================================================
-- Example:
-- If a claim has any surgery event, has_surgery = 1.
-- If a claim has any opioid prescription event, has_opioid = 1.
-- =========================================================

medical_flags as (

    select
        claim_id,

        -- Flag claim if any medical event is surgery
        max(
            case
                when event_type = 'surgery' then 1
                else 0
            end
        ) as has_surgery,

        -- Flag claim if any medical event is opioid prescription
        max(
            case
                when event_type = 'opioid prescription' then 1
                else 0
            end
        ) as has_opioid

    from {{ ref('stg_medical_activity') }}

    group by claim_id

),


-- =========================================================
-- Joined CTE:
-- Join claims to medical activity flags
-- =========================================================
-- LEFT JOIN keeps all claims, even if no medical activity exists.
-- COALESCE converts missing medical flags from NULL to 0.
-- =========================================================

joined as (

    select
        c.claim_id,
        c.date_of_loss,
        c.body_part,
        c.is_litigated,
        c.number_of_priors,

        -- If no matching medical activity exists, treat as no surgery
        coalesce(m.has_surgery, 0) as has_surgery,

        -- If no matching medical activity exists, treat as no opioid activity
        coalesce(m.has_opioid, 0) as has_opioid,

        -- Preserve lineage metadata from claims source
        c._source_file,
        c._ingested_at

    from claims c

    left join medical_flags m
        on c.claim_id = m.claim_id

),


-- =========================================================
-- Scored CTE:
-- Apply business scoring rules
-- =========================================================
-- Scoring example:
-- - Spine/head/back injury: +15
-- - Litigation: +20
-- - 3 or more prior claims: +10
-- - Surgery: +25
-- - Opioid prescription: +15
-- =========================================================

scored as (

    select
        claim_id,
        date_of_loss,
        body_part,
        is_litigated,
        number_of_priors,
        has_surgery,
        has_opioid,

        -- Calculate total complexity score
        case
            when body_part in ('spine', 'head', 'back') then 15
            else 0
        end
        +
        case
            when is_litigated = true then 20
            else 0
        end
        +
        case
            when number_of_priors >= 3 then 10
            else 0
        end
        +
        case
            when has_surgery = 1 then 25
            else 0
        end
        +
        case
            when has_opioid = 1 then 15
            else 0
        end as complexity_score,

        _source_file,
        _ingested_at

    from joined

),


-- =========================================================
-- Final CTE:
-- Assign complexity band based on score
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

        -- Business-friendly complexity category
        case
            when complexity_score >= 50 then 'HIGH'
            when complexity_score >= 30 then 'MEDIUM'
            else 'LOW'
        end as complexity_band,

        _source_file,
        _ingested_at

    from scored

)


-- =========================================================
-- Final Output:
-- Reusable Silver model containing claim-level complexity score
-- =========================================================

select *
from final