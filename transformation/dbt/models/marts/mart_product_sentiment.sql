-- transformation/dbt/models/marts/mart_product_sentiment.sql
-- Business-ready product sentiment mart joining review NLP scores
-- with order data to surface product health signals.

{{
    config(
        materialized='incremental',
        unique_key='product_id',
        on_schema_change='sync_all_columns',
        cluster_by=['last_review_date'],
        tags=['mart', 'nlp', 'product']
    )
}}

with reviews as (
    select
        rs.product_id,
        rs.review_id,
        rs.rating,
        rs.sentiment_label,
        rs.sentiment_score,
        rs.rating_sentiment_match,
        rs.processed_at::date as review_date
    from {{ source('raw', 'review_sentiments') }} rs

    {% if is_incremental() %}
        where rs.processed_at > (select max(last_review_date) from {{ this }})
    {% endif %}
),

order_metrics as (
    select
        product_id,
        sum(line_revenue)        as total_revenue,
        sum(quantity)            as total_units_sold,
        count(distinct order_id) as total_orders
    from {{ ref('stg_orders') }}
    group by 1
),

sentiment_aggregated as (
    select
        product_id,
        count(*)                                                   as total_reviews,
        avg(rating)                                                as avg_rating,
        avg(sentiment_score)                                       as avg_sentiment_score,

        -- sentiment breakdown
        sum(case when sentiment_label = 'POSITIVE' then 1 else 0 end) as positive_count,
        sum(case when sentiment_label = 'NEGATIVE' then 1 else 0 end) as negative_count,
        sum(case when sentiment_label = 'NEUTRAL'  then 1 else 0 end) as neutral_count,

        -- agreement signal: high disagreement may indicate fake reviews
        avg(case when rating_sentiment_match then 1.0 else 0.0 end)   as rating_sentiment_agreement_rate,

        -- recency
        max(review_date)   as last_review_date,
        min(review_date)   as first_review_date,

        -- 30-day rolling
        sum(case when review_date >= dateadd('day', -30, current_date())
                 then 1 else 0 end)                                as reviews_last_30d,
        avg(case when review_date >= dateadd('day', -30, current_date())
                 then sentiment_score else null end)               as avg_sentiment_score_30d

    from reviews
    group by 1
),

final as (
    select
        sa.product_id,

        -- sentiment signals
        sa.total_reviews,
        sa.avg_rating,
        sa.avg_sentiment_score,
        sa.positive_count,
        sa.negative_count,
        sa.neutral_count,
        sa.rating_sentiment_agreement_rate,
        sa.reviews_last_30d,
        sa.avg_sentiment_score_30d,
        sa.last_review_date,
        sa.first_review_date,

        -- derived health score (0-100): blends rating, sentiment, volume
        round(
            (sa.avg_rating / 5.0) * 40
            + sa.avg_sentiment_score * 40
            + least(sa.total_reviews / 100.0, 1.0) * 20
        , 1) as product_health_score,

        -- business metrics
        coalesce(om.total_revenue, 0)      as total_revenue,
        coalesce(om.total_units_sold, 0)   as total_units_sold,
        coalesce(om.total_orders, 0)       as total_orders,

        -- flags
        case
            when sa.avg_rating < 2.5 and sa.total_reviews > 10 then true
            else false
        end as is_at_risk,

        case
            when sa.rating_sentiment_agreement_rate < 0.5
             and sa.total_reviews > 20 then true
            else false
        end as review_integrity_flag,

        current_timestamp() as dbt_loaded_at

    from sentiment_aggregated sa
    left join order_metrics om using (product_id)
)

select * from final
