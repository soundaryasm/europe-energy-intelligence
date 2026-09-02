{{ config(materialized='table') }}

-- Spec 004 dim_country: one row per MVP country, stable deterministic key.
-- Source: the country_reference seed (see seeds/_seeds.yml for the
-- known maintenance risk of keeping it in sync with the Python-side
-- src/config/countries.yaml + src/config/entsoe_domains.yaml).

select
    upper(country_code) as country_key,
    upper(country_code) as country_code,
    country_name,
    reference_location,
    timezone,
    entsoe_domain
from {{ ref('country_reference') }}
