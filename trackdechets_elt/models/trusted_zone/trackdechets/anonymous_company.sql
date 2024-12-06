{{
  config(
    materialized = 'table',
    indexes = [
        { "columns" : ["id"], "unique": True },
        { "columns" : ["siret"], "unique": True },
    ]
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'anonymous_company_raw') }}
),

renamed as (
    select
        id,
        siret,
        name,
        address,
        "codeNaf"     as code_naf,
        "libelleNaf"  as libelle_naf,
        "codeCommune" as code_commune,
        "vatNumber"   as vat_number,
        "orgId"       as org_id
    from source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select * from renamed
