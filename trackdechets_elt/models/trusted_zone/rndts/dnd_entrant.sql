{{
  config(
    materialized = 'table',
    indexes=[
        {"columns":['id'],"unique":True},
        {"columns":['created_date']},
        {"columns":['etablissement_numero_identification']},
        {"columns":['date_reception']},
        {"columns":['numeros_indentification_transporteurs'],"type":"GIN"},
        ]

    )
}}

with source as (
    select * from {{ source('raw_zone_rndts', 'dnd_entrant') }}
),

transporter_source as (
    select
        dnd_entrant_id,
        ARRAY_AGG(
            transporteur_numero_identification::text
        ) as numeros_indentification_transporteurs
    from
        {{ ref("dnd_entrant_transporteur") }}
    group by 1

),

renamed as (
    select
        {{ adapter.quote("dnd_entrant_id") }} as id,
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("date_reception") }},
        {{ adapter.quote("is_dechet_pop") }},
        {{ adapter.quote("denomination_usuelle") }},
        {{ adapter.quote("heure_pesee") }},
        {{ adapter.quote("last_modified_date") }},
        {{ adapter.quote("numero_document") }},
        {{ adapter.quote("numero_notification") }},
        {{ adapter.quote("numero_saisie") }},
        {{ adapter.quote("quantite") }},
        {{ adapter.quote("code_traitement") }},
        {{ adapter.quote("etablissement_id") }},
        {{ adapter.quote("created_by_id") }},
        {{ adapter.quote("last_modified_by_id") }},
        {{ adapter.quote("unite_code") }}     as code_unite,
        {{ adapter.quote("public_id") }},
        {{ adapter.quote("delegation_id") }},
        {{ adapter.quote("origine") }},
        {{ adapter.quote("code_dechet_bale") }},
        {{ adapter.quote("identifiant_metier") }},
        {{ adapter.quote("canceled_by_id") }},
        {{ adapter.quote("canceled_comment") }},
        {{ adapter.quote("canceled_date") }},
        {{ adapter.quote("import_id") }},
        {{ adapter.quote("producteur_type") }},
        {{ adapter.quote("producteur_numero_identification") }},
        {{ adapter.quote("producteur_raison_sociale") }},
        {{ adapter.quote("producteur_adresse_libelle") }},
        {{ adapter.quote("producteur_adresse_commune") }},
        {{ adapter.quote("producteur_adresse_code_postal") }},
        {{ adapter.quote("producteur_adresse_pays") }},
        {{ adapter.quote("expediteur_type") }},
        {{ adapter.quote("expediteur_numero_identification") }},
        {{ adapter.quote("expediteur_raison_sociale") }},
        {{ adapter.quote("expediteur_adresse_prise_en_charge") }},
        {{ adapter.quote("expediteur_adresse_libelle") }},
        {{ adapter.quote("expediteur_adresse_commune") }},
        {{ adapter.quote("expediteur_adresse_code_postal") }},
        {{ adapter.quote("expediteur_adresse_pays") }},
        {{ adapter.quote("eco_organisme_type") }},
        {{ adapter.quote("eco_organisme_numero_identification") }},
        {{ adapter.quote("eco_organisme_raison_sociale") }},
        {{ adapter.quote("courtier_type") }},
        {{ adapter.quote("courtier_numero_identification") }},
        {{ adapter.quote("courtier_raison_sociale") }},
        {{ adapter.quote("courtier_numero_recepisse") }}
    from source
)

select
    r.id,
    r.created_year_utc,
    r.code_dechet,
    r.created_date,
    r.date_reception::date,
    r.is_dechet_pop,
    r.denomination_usuelle,
    r.heure_pesee::time,
    r.last_modified_date,
    r.numero_document,
    r.numero_notification,
    r.numero_saisie,
    r.quantite,
    r.code_traitement,
    r.etablissement_id,
    e.numero_identification as etablissement_numero_identification,
    r.created_by_id,
    r.last_modified_by_id,
    r.code_unite,
    r.public_id,
    r.delegation_id,
    r.origine,
    r.code_dechet_bale,
    r.identifiant_metier,
    r.canceled_by_id,
    r.canceled_comment,
    r.canceled_date,
    r.import_id,
    r.producteur_type,
    r.producteur_numero_identification,
    r.producteur_raison_sociale,
    r.producteur_adresse_libelle,
    r.producteur_adresse_commune,
    r.producteur_adresse_code_postal,
    r.producteur_adresse_pays,
    r.expediteur_type,
    r.expediteur_numero_identification,
    r.expediteur_raison_sociale,
    r.expediteur_adresse_prise_en_charge,
    r.expediteur_adresse_libelle,
    r.expediteur_adresse_commune,
    r.expediteur_adresse_code_postal,
    r.expediteur_adresse_pays,
    r.eco_organisme_type,
    r.eco_organisme_numero_identification,
    r.eco_organisme_raison_sociale,
    r.courtier_type,
    r.courtier_numero_identification,
    r.courtier_raison_sociale,
    r.courtier_numero_recepisse,
    t.numeros_indentification_transporteurs
from renamed as r
left join transporter_source as t on r.id = t.dnd_entrant_id
left join {{ ref('etablissement') }} as e on r.etablissement_id = e.id
