-- make sql in 'core' as tables since these are exposed to BI tools. Tables being more
-- efficient
{{ config(materialized="table") }}

select locationid, borough, zone, replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref("taxi_zone_lookup") }}
