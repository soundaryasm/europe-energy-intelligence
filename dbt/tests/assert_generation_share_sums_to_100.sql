-- Spec 007 Cross-Dataset Validation: for each country+date, generation
-- share percentages should approximately sum to 100% (small numerical
-- tolerance). A returned row is a failing country/date.
select
    country_key,
    date_key,
    sum(generation_share_pct) as total_share_pct
from {{ ref('fact_generation_mix_daily') }}
group by country_key, date_key
having abs(sum(generation_share_pct) - 100) > 0.5
