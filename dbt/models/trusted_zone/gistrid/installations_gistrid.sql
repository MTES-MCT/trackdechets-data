{{
  config(
    materialized = 'view',
    )
}}

with source as (
    select * from {{ source('raw_zone_gistrid', 'installations') }}
),

renamed as (
    select
        "Numéro GISTRID"                                               as numero_gistrid,
        "Type d'opérateur"                                             as type_operateur,
        "Nom de la société"                                            as nom_societe,
        "N° d'enregistrement"                                          as numero_enregistrement,
        "Adresse"                                                      as adresse,
        "Pays"                                                         as pays,
        "Code postal"                                                  as code_postal,
        "Commune"                                                      as commune,
        "Ville d'implantation de l'établissement"                      as ville_implatation_etablissement,
        "Pays de l'établissement"                                      as pays_etablissement,
        "Installation de valorisation bénéficiant du consentement pré" as beneficie_consentement_prealable,
        "Statut de l'opérateur"                                        as statut_operateur,
        "Date du statut"                                               as date_statut,
        "Département de l'établissement"                               as departement_etablissement,
        "SIRET"                                                        as siret,
        "S3IC"                                                         as s3ic,
        "Raison de refus"                                              as raison_refus,
        "Nombre de notifications réservées"                            as nombre_notifications_reservees,
        "Indicateur d'activité"                                        as indicateur_activite
    from source
)

select * from renamed
