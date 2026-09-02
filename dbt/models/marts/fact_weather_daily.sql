{{ config(materialized='table') }}

-- Spec 004 fact_weather_daily. Grain: country + date.

select
    dc.country_key,
    dd.date_key,
    w.avg_temperature_c,
    w.avg_wind_speed_kmh,
    w.solar_radiation_mj_m2,
    w.reference_location
from {{ source('silver', 'silver_weather_daily') }} w
inner join {{ ref('dim_country') }} dc on dc.country_code = w.country_code
inner join {{ ref('dim_date') }} dd on dd.date = w.local_date
