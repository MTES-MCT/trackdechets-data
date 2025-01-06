with source as (
    select * from {{ source('raw_zone_rndts', 'dnd_sortant_transporteur') }}
),

renamed as (
    select
        {{ adapter.quote("dnd_sortant_id") }},
        {{ adapter.quote("dnd_sortant_created_year_utc") }},
        {{ adapter.quote("transporteur_type") }},
        {{ adapter.quote("transporteur_numero_identification") }},
        {{ adapter.quote("transporteur_raison_sociale") }},
        {{ adapter.quote("transporteur_numero_recepisse") }},
        {{ adapter.quote("transporteur_adresse_libelle") }},
        {{ adapter.quote("transporteur_adresse_commune") }},
        {{ adapter.quote("transporteur_adresse_code_postal") }},
        {{ adapter.quote("transporteur_adresse_pays") }}
    from source
)

select
    dnd_sortant_id,
    dnd_sortant_created_year_utc,
    transporteur_type,
    transporteur_numero_identification,
    transporteur_raison_sociale,
    transporteur_numero_recepisse,
    transporteur_adresse_libelle,
    transporteur_adresse_commune,
    transporteur_adresse_code_postal,
    transporteur_adresse_pays
from renamed
