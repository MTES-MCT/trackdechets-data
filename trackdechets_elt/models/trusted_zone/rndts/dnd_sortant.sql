{{
  config(
    materialized = 'table',
    indexes=[
        {"columns":['id'],"unique":True},
        {"columns":['created_date']},
        {"columns":['etablissement_numero_identification']},
        {"columns":['producteur_numero_identification']},
        {"columns":['date_expedition']},
        {"columns":['numeros_indentification_transporteurs'],"type":"GIN"},
        ]

    )
}}

with source as (
    select * from {{ source('raw_zone_rndts', 'dnd_sortant') }}
),

transporter_source as (
    select
        dnd_sortant_id,
        ARRAY_AGG(
            transporteur_numero_identification::text
        ) as numeros_indentification_transporteurs
    from
        {{ ref("dnd_sortant_transporteur") }}
    group by 1

),

renamed as (
    select
        {{ adapter.quote("dnd_sortant_id") }} as id,
        {{ adapter.quote("created_year_utc") }},
        {{ adapter.quote("code_dechet") }},
        {{ adapter.quote("created_date") }},
        {{ adapter.quote("date_expedition") }},
        {{ adapter.quote("is_dechet_pop") }},
        {{ adapter.quote("denomination_usuelle") }},
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
        {{ adapter.quote("qualification_code") }},
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
        {{ adapter.quote("destinataire_type") }},
        {{ adapter.quote("destinataire_numero_identification") }},
        {{ adapter.quote("destinataire_raison_sociale") }},
        {{ adapter.quote("destinataire_adresse_destination") }},
        {{ adapter.quote("destinataire_adresse_libelle") }},
        {{ adapter.quote("destinataire_adresse_commune") }},
        {{ adapter.quote("destinataire_adresse_code_postal") }},
        {{ adapter.quote("destinataire_adresse_pays") }},
        {{ adapter.quote("etablissement_origine_adresse_prise_en_charge") }},
        {{ adapter.quote("etablissement_origine_adresse_libelle") }},
        {{ adapter.quote("etablissement_origine_adresse_commune") }},
        {{ adapter.quote("etablissement_origine_adresse_code_postal") }},
        {{ adapter.quote("etablissement_origine_adresse_pays") }},
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
    r.date_expedition::date,
    r.is_dechet_pop,
    r.denomination_usuelle,
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
    r.qualification_code,
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
    r.destinataire_type,
    r.destinataire_numero_identification,
    r.destinataire_raison_sociale,
    r.destinataire_adresse_destination,
    r.destinataire_adresse_libelle,
    r.destinataire_adresse_commune,
    r.destinataire_adresse_code_postal,
    r.destinataire_adresse_pays,
    r.etablissement_origine_adresse_prise_en_charge,
    r.etablissement_origine_adresse_libelle,
    r.etablissement_origine_adresse_commune,
    r.etablissement_origine_adresse_code_postal,
    r.etablissement_origine_adresse_pays,
    r.eco_organisme_type,
    r.eco_organisme_numero_identification,
    r.eco_organisme_raison_sociale,
    r.courtier_type,
    r.courtier_numero_identification,
    r.courtier_raison_sociale,
    r.courtier_numero_recepisse,
    t.numeros_indentification_transporteurs
from renamed as r
left join transporter_source as t on r.id = t.dnd_sortant_id
left join {{ ref('etablissement') }} as e on r.etablissement_id = e.id
