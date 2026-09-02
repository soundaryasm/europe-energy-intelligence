{{ config(materialized='table') }}

-- Spec 004 dim_date: one row per calendar date. The covered range is a
-- dbt var (dbt_project.yml: dim_date_start_date/dim_date_end_date), not
-- hard-coded here, so it can be widened without editing this model.

with date_spine as (
    select explode(sequence(
        to_date('{{ var("dim_date_start_date") }}'),
        to_date('{{ var("dim_date_end_date") }}'),
        interval 1 day
    )) as date_day
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day as date,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    day(date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    weekofyear(date_day) as week_of_year,
    -- Spark SQL dayofweek(): 1 = Sunday ... 7 = Saturday.
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend
from date_spine
