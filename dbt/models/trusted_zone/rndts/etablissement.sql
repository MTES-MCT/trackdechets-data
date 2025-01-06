with source as (
    select * from {{ source('raw_zone_rndts', 'etablissement') }}
),

renamed as (
    select
        {{ adapter.quote("etablissement_id") }},
        {{ adapter.quote("numero_identification") }},
        {{ adapter.quote("raison_sociale") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("type_code") }},
        {{ adapter.quote("timezone_code") }},
        {{ adapter.quote("public_id") }},
        {{ adapter.quote("created_by_id") }},
        {{ adapter.quote("last_modified_by_id") }},
        {{ adapter.quote("disabled_date") }},
        {{ adapter.quote("disabled_reason_code") }},
        {{ adapter.quote("etablissement_validateur_id") }}
    from source
)

select
    etablissement_id as id,
    numero_identification,
    raison_sociale,
    created_date,
    last_modified_date,
    type_code,
    timezone_code,
    public_id,
    created_by_id,
    last_modified_by_id,
    disabled_date,
    disabled_reason_code,
    etablissement_validateur_id
from renamed
