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
    from {{ source('trackdechets_production', 'vhu_agrement_raw') }}
),

renamed as (
    select
        id,
        "agrementNumber" as agrement_number,
        department
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    agrement_number,
    department
from renamed
