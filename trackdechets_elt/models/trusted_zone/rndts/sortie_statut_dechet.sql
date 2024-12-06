{{
  config(
    materialized = 'table',
    indexes=[
        {"columns":['id'],"unique":True},
        {"columns":['created_date']},
        {"columns":['etablissement_numero_identification']},
        ]

    )
}}

with source as (
    select * from {{ source('raw_zone_rndts', 'sortie_statut_dechet') }}
),

renamed as (
    select
        {{ adapter.quote("sortie_statut_dechet_id") }} as id,
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("public_id") }},
        {{ adapter.quote("identifiant_metier") }},
        {{ adapter.quote("etablissement_id") }},
        {{ adapter.quote("created_by_id") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("last_modified_by_id") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("delegation_id") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("code_dechet_bale") }},
        {{ adapter.quote("date_utilisation") }},
        {{ adapter.quote("date_expedition") }},
        {{ adapter.quote("nature") }},
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("unite_code") }}              as code_unite,
        {{ adapter.quote("date_traitement") }},
        {{ adapter.quote("date_fin_traitement") }},
        {{ adapter.quote("code_traitement") }},
        {{ adapter.quote("qualification_code") }},
        {{ adapter.quote("reference_acte_administratif") }},
        {{ adapter.quote("origine") }},
        {{ adapter.quote("canceled_by_id") }},
        {{ adapter.quote("canceled_comment") }},
        {{ adapter.quote("canceled_date") }},
        {{ adapter.quote("import_id") }},
        {{ adapter.quote("destinataire_type") }},
        {{ adapter.quote("destinataire_numero_identification") }},
        {{ adapter.quote("destinataire_raison_sociale") }},
        {{ adapter.quote("destinataire_adresse_destination") }},
        {{ adapter.quote("destinataire_adresse_libelle") }},
        {{ adapter.quote("destinataire_adresse_commune") }},
        {{ adapter.quote("destinataire_adresse_code_postal") }},
        {{ adapter.quote("destinataire_adresse_pays") }}
    from source
)

select
    r.id,
    r.created_year_utc,
    r.public_id,
    r.identifiant_metier,
    r.etablissement_id,
    e.numero_identification as etablissement_numero_identification,
    r.created_by_id,
    r.created_date,
    r.last_modified_by_id,
    r.last_modified_date,
    r.delegation_id,
    r.denomination_usuelle,
    r.code_dechet,
    r.code_dechet_bale,
    r.date_utilisation,
    r.date_expedition,
    r.nature,
    r.quantite,
    r.code_unite,
    r.date_traitement,
    r.date_fin_traitement,
    r.code_traitement,
    r.qualification_code,
    r.reference_acte_administratif,
    r.origine,
    r.canceled_by_id,
    r.canceled_comment,
    r.canceled_date,
    r.import_id,
    r.destinataire_type,
    r.destinataire_numero_identification,
    r.destinataire_raison_sociale,
    r.destinataire_adresse_destination,
    r.destinataire_adresse_libelle,
    r.destinataire_adresse_commune,
    r.destinataire_adresse_code_postal,
    r.destinataire_adresse_pays
from renamed as r
left join {{ ref('etablissement') }} as e on r.etablissement_id = e.id
