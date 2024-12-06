{{
  config(
    materialized = 'table',
    indexes = [
        { "columns" : ["id"], "unique": True },
    ]
    )
}}
with source as (
    select * from {{ source('trackdechets_production', 'broker_receipt_raw') }}
)

select
    id,
    "receiptNumber" as receipt_number,
    "validityLimit" as validity_limit,
    department
from
    source
where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
