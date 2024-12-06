{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True}
    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'trader_receipt_raw') }}
),

renamed as (
    select
        id,
        "receiptNumber"                      as receipt_number,
        cast("validityLimit" as timestamptz) as validity_limit,
        department
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    receipt_number,
    validity_limit,
    department
from renamed
