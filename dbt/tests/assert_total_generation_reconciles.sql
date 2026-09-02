-- Spec 007 Cross-Dataset Validation: fact_energy_daily.total_generation_mwh
-- must reconcile with the total represented by fact_generation_mix_daily,
-- within a small numerical tolerance.
with mix_total as (
    select country_key, date_key, sum(generation_mwh) as total_mwh
    from {{ ref('fact_generation_mix_daily') }}
    group by country_key, date_key
)

select
    e.country_key,
    e.date_key,
    e.total_generation_mwh,
    mix_total.total_mwh
from {{ ref('fact_energy_daily') }} e
inner join mix_total
    on mix_total.country_key = e.country_key
    and mix_total.date_key = e.date_key
where abs(e.total_generation_mwh - mix_total.total_mwh) > 0.5
