{{ config(materialized='table') }}

-- Spec 004 fact_generation_mix_daily. Grain: country + date + production_type.
--
-- Spec 004 "Trusted Silver Inputs": a `partial` per-production-type Silver
-- row is excluded here entirely (no Gold row for it — the same treatment
-- as a genuinely missing country/date) rather than being published as a
-- trustworthy generation figure. The share-percentage denominator also
-- requires every production type for that country/date to be complete —
-- otherwise it would look like an ordinary percentage while actually
-- being computed from an understated partial total.

with complete_rows as (
    select *
    from {{ source('silver', 'silver_generation_mix_daily') }}
    where completeness_status = 'complete'
),

daily_totals as (
    select
        country_code,
        local_date,
        sum(generation_mwh) as total_generation_mwh,
        min(case when completeness_status = 'complete' then 1 else 0 end) as all_types_complete
    from {{ source('silver', 'silver_generation_mix_daily') }}
    group by country_code, local_date
)

select
    dc.country_key,
    dd.date_key,
    g.normalized_production_type as production_type,
    g.generation_mwh,
    g.renewable_flag,
    -- Spec 004: "Handle zero-generation cases explicitly. Do not
    -- silently divide by zero."
    case
        when t.all_types_complete = 1 and t.total_generation_mwh > 0
            then g.generation_mwh / t.total_generation_mwh * 100
        else null
    end as generation_share_pct
from complete_rows g
inner join daily_totals t
    on t.country_code = g.country_code
    and t.local_date = g.local_date
inner join {{ ref('dim_country') }} dc on dc.country_code = g.country_code
inner join {{ ref('dim_date') }} dd on dd.date = g.local_date
