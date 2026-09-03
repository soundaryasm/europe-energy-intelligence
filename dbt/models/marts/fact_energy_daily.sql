{{ config(materialized='table') }}

-- Spec 004 fact_energy_daily. Grain: country + date. Combines daily
-- demand, day-ahead price statistics, and generation totals (for
-- renewable_generation_pct) from three Silver sources.

-- Spec 004 "Trusted Silver Inputs": only `complete` Silver rows may
-- contribute to trusted Gold metrics. A `partial` row's daily_demand_mwh
-- (etc.) is a real but understated number, and must not silently flow
-- through here — excluding it leaves the metric null instead (see
-- `all_keys` below), per Spec 004's null-vs-zero semantics.
with demand as (
    select country_code, local_date, daily_demand_mwh
    from {{ source('silver', 'silver_energy_demand_daily') }}
    where completeness_status = 'complete'
),

price as (
    select
        country_code,
        local_date,
        avg_day_ahead_price_eur_mwh,
        min_day_ahead_price_eur_mwh,
        max_day_ahead_price_eur_mwh
    from {{ source('silver', 'silver_energy_price_daily') }}
    where completeness_status = 'complete'
),

-- A day's total is only trustworthy if EVERY production-type series for
-- that country/date is complete — summing only the complete rows would
-- silently understate the total instead of leaving it null, which is
-- the exact failure mode Spec 004 forbids.
generation_daily as (
    select
        country_code,
        local_date,
        sum(generation_mwh) as total_generation_mwh_raw,
        sum(case when renewable_flag then generation_mwh else 0 end) as renewable_generation_mwh_raw,
        min(case when completeness_status = 'complete' then 1 else 0 end) as all_types_complete
    from {{ source('silver', 'silver_generation_mix_daily') }}
    group by country_code, local_date
),

generation_totals as (
    select
        country_code,
        local_date,
        case when all_types_complete = 1 then total_generation_mwh_raw end as total_generation_mwh,
        case when all_types_complete = 1 then renewable_generation_mwh_raw end as renewable_generation_mwh
    from generation_daily
),

all_keys as (
    select country_code, local_date from demand
    union
    select country_code, local_date from price
    union
    select country_code, local_date from generation_totals
)

select
    dc.country_key,
    dd.date_key,
    demand.daily_demand_mwh,
    price.avg_day_ahead_price_eur_mwh,
    price.min_day_ahead_price_eur_mwh,
    price.max_day_ahead_price_eur_mwh,
    generation_totals.total_generation_mwh,
    generation_totals.renewable_generation_mwh,
    -- Spec 004: "Handle zero-generation cases explicitly. Do not
    -- silently divide by zero." NULLIF makes a zero-generation day
    -- produce NULL, never a fabricated 0%.
    case
        when generation_totals.total_generation_mwh is null then null
        else generation_totals.renewable_generation_mwh
             / nullif(generation_totals.total_generation_mwh, 0) * 100
    end as renewable_generation_pct
from all_keys
left join demand
    on demand.country_code = all_keys.country_code
    and demand.local_date = all_keys.local_date
left join price
    on price.country_code = all_keys.country_code
    and price.local_date = all_keys.local_date
left join generation_totals
    on generation_totals.country_code = all_keys.country_code
    and generation_totals.local_date = all_keys.local_date
inner join {{ ref('dim_country') }} dc
    on dc.country_code = all_keys.country_code
inner join {{ ref('dim_date') }} dd
    on dd.date = all_keys.local_date
