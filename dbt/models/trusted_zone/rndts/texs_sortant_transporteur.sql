with source as (
    select * from {{ source('raw_zone_rndts', 'texs_sortant_transporteur') }}
),

renamed as (
    select
        {{ adapter.quote("texs_sortant_id") }}                    as texs_sortant_id,
        {{ adapter.quote("texs_sortant_created_year_utc") }}      as texs_sortant_created_year_utc,
        {{ adapter.quote("transporteur_type") }}                  as transporteur_type,
        {{ adapter.quote("transporteur_numero_identification") }} as transporteur_numero_identification,
        {{ adapter.quote("transporteur_raison_sociale") }}        as transporteur_raison_sociale,
        {{ adapter.quote("transporteur_numero_recepisse") }}      as transporteur_numero_recepisse,
        {{ adapter.quote("transporteur_adresse_libelle") }}       as transporteur_adresse_libelle,
        {{ adapter.quote("transporteur_adresse_commune") }}       as transporteur_adresse_commune,
        {{ adapter.quote("transporteur_adresse_code_postal") }}   as transporteur_adresse_code_postal,
        {{ adapter.quote("transporteur_adresse_pays") }}          as transporteur_adresse_pays
    from source
)

select
    texs_sortant_id,
    texs_sortant_created_year_utc,
    transporteur_type,
    transporteur_numero_identification,
    transporteur_raison_sociale,
    transporteur_numero_recepisse,
    transporteur_adresse_libelle,
    transporteur_adresse_commune,
    transporteur_adresse_code_postal,
    transporteur_adresse_pays
from renamed
