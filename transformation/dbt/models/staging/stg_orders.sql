-- transformation/dbt/models/staging/stg_orders.sql
-- Cleans and types the raw orders table landed by Spark Streaming.
-- Incremental model: only processes new records since last dbt run.

{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='sync_all_columns',
        cluster_by=['order_date'],
        tags=['staging', 'orders']
    )
}}

with source as (
    select * from {{ source('raw', 'orders') }}

    {% if is_incremental() %}
        where event_ts > (select max(event_ts) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- surrogate key for the exploded line-item grain
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_item_id,

        -- order keys
        order_id,
        customer_id,
        product_id,
        product_name,

        -- dimensions
        lower(trim(category))          as category,
        lower(trim(customer_segment))  as customer_segment,
        lower(trim(payment_method))    as payment_method,
        lower(trim(status))            as order_status,
        upper(trim(currency))          as currency,

        -- measures
        quantity::integer              as quantity,
        unit_price::float              as unit_price,
        (quantity * unit_price)::float as line_revenue,
        total_amount::float            as order_total_amount,

        -- timestamps
        event_ts::timestamp_ntz        as event_ts,
        event_ts::date                 as order_date,
        date_trunc('hour', event_ts)   as order_hour,

        -- metadata
        current_timestamp()            as dbt_loaded_at

    from source
),

validated as (
    select *
    from renamed
    where
        order_id         is not null
        and customer_id  is not null
        and product_id   is not null
        and quantity      > 0
        and unit_price    > 0
        and line_revenue  > 0
)

select * from validated
