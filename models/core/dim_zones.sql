{{ config(materialized='table' )}}  -- make sql in 'core' as tables since these are exposed to BI tools. Tables being more efficient

select
    "locationid",
    "Borough",
    "Zone",
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('taxi_zone_lookup') }}