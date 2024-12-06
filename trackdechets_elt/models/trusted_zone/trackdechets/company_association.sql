{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["company_id"]},
        { "columns": ["user_id"]},
    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'company_association_raw') }}
),

renamed as (
    select
        id,
        "role",
        "companyId"             as company_id,
        "userId"                as user_id,
        "createdAt"             as created_at,
        "automaticallyAccepted" as automatically_accepted
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    "role",
    company_id,
    user_id,
    created_at,
    automatically_accepted
from renamed
