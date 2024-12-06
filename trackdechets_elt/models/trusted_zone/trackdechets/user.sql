{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["email"]}
    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'user_raw') }}
),

renamed as (
    select
        id,
        email,
        "name",
        phone,
        "createdAt"            as created_at,
        "updatedAt"            as updated_at,
        "isActive"             as is_active,
        "activatedAt"          as activated_at,
        "firstAssociationDate" as first_association_date,
        "isAdmin"              as is_admin,
        "isRegistreNational"   as is_registre_national,
        "governmentAccountId"  as government_account_id
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    email,
    "name",
    phone,
    created_at,
    updated_at,
    is_active,
    activated_at,
    first_association_date,
    is_admin,
    is_registre_national,
    government_account_id
from renamed
