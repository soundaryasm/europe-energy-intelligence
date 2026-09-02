-- Spec 007 Cross-Dataset Validation: fact_energy_daily.renewable_generation_mwh
-- must equal the sum of renewable-flagged generation in
-- fact_generation_mix_daily, within a small numerical tolerance.
with mix_renewable as (
    select country_key, date_key, sum(generation_mwh) as renewable_mwh
    from {{ ref('fact_generation_mix_daily') }}
    where renewable_flag = true
    group by country_key, date_key
)

select
    e.country_key,
    e.date_key,
    e.renewable_generation_mwh,
    mix_renewable.renewable_mwh
from {{ ref('fact_energy_daily') }} e
inner join mix_renewable
    on mix_renewable.country_key = e.country_key
    and mix_renewable.date_key = e.date_key
where abs(e.renewable_generation_mwh - mix_renewable.renewable_mwh) > 0.5
