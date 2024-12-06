{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["siret"], "unique": True}

    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'eco_organisme_raw') }}
),

renamed as (
    select
        id,
        siret,
        "name",
        address,
        "handleBsdasri" as handle_bsdasri
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    siret,
    "name",
    address,
    handle_bsdasri
from renamed
