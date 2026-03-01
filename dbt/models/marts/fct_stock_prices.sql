-- Fact table incremental: grain = (ticker, trade_date)
{{ config(
    materialized='incremental',
    unique_key=['ticker_symbol', 'trade_date'],
    incremental_strategy='merge',
    cluster_by=['ticker_symbol'],
    tags=['daily', 'mart', 'fact']
) }}

with prices as (
    select * from {{ ref('stg_stock_prices_daily') }}
),

final as (
    select
        ticker_symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        trading_volume,
        daily_return_pct,
        moving_avg_5d,
        moving_avg_20d,
        case
            when close_price > moving_avg_5d
             and moving_avg_5d > moving_avg_20d then 'bullish'
            when close_price < moving_avg_5d
             and moving_avg_5d < moving_avg_20d then 'bearish'
            else 'neutral'
        end                   as trend_signal,
        data_quality_flag,
        dbt_updated_at
    from prices
)

select * from final

{% if is_incremental() %}
    where trade_date > (select max(trade_date) from {{ this }})
{% endif %}
