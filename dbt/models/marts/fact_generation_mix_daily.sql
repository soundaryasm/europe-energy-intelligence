{{ config(materialized='table') }}

-- Spec 004 fact_generation_mix_daily. Grain: country + date + production_type.

with totals as (
    select country_code, local_date, sum(generation_mwh) as total_generation_mwh
    from {{ source('silver', 'silver_generation_mix_daily') }}
    group by country_code, local_date
)

select
    dc.country_key,
    dd.date_key,
    g.normalized_production_type as production_type,
    g.generation_mwh,
    g.renewable_flag,
    -- Spec 004: "Handle zero-generation cases explicitly."
    case
        when totals.total_generation_mwh is null or totals.total_generation_mwh = 0 then null
        else g.generation_mwh / totals.total_generation_mwh * 100
    end as generation_share_pct
from {{ source('silver', 'silver_generation_mix_daily') }} g
inner join totals
    on totals.country_code = g.country_code
    and totals.local_date = g.local_date
inner join {{ ref('dim_country') }} dc on dc.country_code = g.country_code
inner join {{ ref('dim_date') }} dd on dd.date = g.local_date
