{{ config(materialized="view") }}

select
    -- identifier
    {{ dbt_utils.surrogate_key(["'VendorID'", "tpep_pickup_datetime"]) }} as tripid,
    safe_cast("VendorID" as integer) as vendorid,
    safe_cast("RatecodeID" as integer) as ratecodeid,
    safe_cast("PULocationID" as integer) as puocationid,
    safe_cast("DOLocationID" as integer) as dolocationid,

    -- timestamp
    cast(tpep_pickup_datetime as timestamp) as tpep_pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as tpep_dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- cast(trip_type as integer) as trip_type,
    -- pyment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description,
    cast(congestion_surcharge as numeric) as congestion_surcharge

from {{ source("staging", "yellow_tripdata") }}
where "VendorID" is not null


{% if var("is_test_run", default=true) %} limit 100 {% endif %}
