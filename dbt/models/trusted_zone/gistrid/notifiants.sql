{{
  config(
    materialized = 'view',
    )
}}

with source as (
    select * from {{ source('raw_zone_gistrid', 'notifiants') }}
),

renamed as (
    select
        {{ adapter.quote("Numéro GISTRID") }}                                               as numero_gistrid,
        {{ adapter.quote("Type d'opérateur") }}                                             as type_operateur,
        {{ adapter.quote("Nom de la société") }}                                            as nom_societe,
        {{ adapter.quote("N° d'enregistrement") }}                                          as numero_enregistrement,
        {{ adapter.quote("Adresse") }}                                                      as adresse,
        {{ adapter.quote("Pays") }}                                                         as pays,
        {{ adapter.quote("Code postal") }}                                                  as code_postal,
        {{ adapter.quote("Commune") }}                                                      as commune,
        {{ adapter.quote("Ville d'implantation de l'établissement") }}                      as ville_implatation_etablissement,
        {{ adapter.quote("Pays de l'établissement") }}                                      as pays_etablissement,
        {{ adapter.quote("Installation de valorisation bénéficiant du consentement pré") }} as beneficie_consentement_prealable,
        {{ adapter.quote("Statut de l'opérateur") }}                                        as statut_operateur,
        {{ adapter.quote("Date du statut") }}                                               as date_statut,
        {{ adapter.quote("Département de l'établissement") }}                               as departement_etablissement,
        {{ adapter.quote("SIRET") }}                                                        as siret,
        {{ adapter.quote("S3IC") }}                                                         as s3ic,
        {{ adapter.quote("Raison de refus") }}                                              as raison_refus,
        {{ adapter.quote("Nombre de notifications réservées") }}                            as nombre_notifications_reservees,
        {{ adapter.quote("Indicateur d'activité") }}                                        as indicateur_activite

    from source
)

select * from renamed
