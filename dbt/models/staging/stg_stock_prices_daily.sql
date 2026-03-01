-- Staging: tipagem e renomeacao 1:1 com a fonte Silver
{{ config(materialized='view', tags=['daily', 'staging']) }}

with source as (
    select * from {{ source('silver', 'stock_prices_daily') }}
),

renamed as (
    select
        symbol                as ticker_symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume                as trading_volume,
        price_range,
        daily_return_pct,
        ma_5d                 as moving_avg_5d,
        ma_20d                as moving_avg_20d,
        dq_flag               as data_quality_flag,
        ingested_at,
        source_system,
        current_timestamp()   as dbt_updated_at
    from source
    where dq_flag != 'invalid_price'
      and trade_date >= '2020-01-01'
)

select * from renamed
