{{
  config(
    materialized = 'table',
    indexes=[{"columns":["numero_notification"], "unique":True},
    {"columns":["numero_gistrid_notifiant"]},
    {"columns":["numero_gistrid_installation_traitement"]}]
    )
}}

with source as (
    select *
    from {{ source('raw_zone_gistrid', 'notifications') }}
),

renamed as (
    select
        {{ adapter.quote("numéro de notification") }}                         as numero_notification,
        {{ adapter.quote("type de dossier") }}                                as type_dossier,
        {{ adapter.quote("état du dossier") }}                                as etat_dossier,
        {{ adapter.quote("département du dossier") }}                         as departement_dossier,
        {{ adapter.quote("nom du notifiant") }}                               as nom_notifiant,
        {{ adapter.quote("numéro GISTRID du notifiant") }}                    as numero_gistrid_notifiant,
        {{ adapter.quote("commune du notifiant") }}                           as commune_notifiant,
        {{ adapter.quote("pays du notifiant") }}                              as pays_notifiant,
        {{ adapter.quote("nom du producteur") }}                              as nom_producteur,
        {{ adapter.quote("commune du producteur") }}                          as commune_producteur,
        {{ adapter.quote("pays du producteur") }}                             as pays_producteur,
        {{ adapter.quote("numéro SIRET de l'installation de traitement") }}   as siret_installation_traitement,
        {{ adapter.quote("nom de l'installation de traitement") }}            as nom_installation_traitement,
        {{ adapter.quote("numéro GISTRID de l'installation de traitement") }} as numero_gistrid_installation_traitement,
        {{ adapter.quote("commune de l'installation de traitement") }}        as commune_installation_traitement,
        {{ adapter.quote("pays de l'installation de traitement") }}           as pays_installation_traitement,
        {{ adapter.quote("pays de transit") }}                                as pays_transit,
        {{ adapter.quote("date du consentement français") }}                  as date_consentement_francais,
        {{ adapter.quote("date autorisée du début des transferts") }}         as date_autorisee_debut_transferts,
        {{ adapter.quote("date autorisée de la fin des transferts") }}        as date_autorisee_fin_transferts,
        {{ adapter.quote("code D/R") }}                                       as code_d_r,
        {{ adapter.quote("code CB") }}                                        as code_cb,
        {{ adapter.quote("code CED") }}                                       as code_ced,
        {{ adapter.quote("code OCDE") }}                                      as code_ocde,
        {{ adapter.quote("code Y") }}                                         as code_y,
        {{ adapter.quote("code H") }}                                         as code_h,
        {{ adapter.quote("Caractéristiques physiques") }}                     as caracteristiques_physiques,
        {{ adapter.quote("quantité autorisée") }}::float                      as quantite_autorisee,
        {{ adapter.quote("unité") }}                                          as unite,
        {{ adapter.quote("Nombre total de transferts autorisés") }}::int      as nombre_total_transferts_autorises,
        {{ adapter.quote("somme des quantités reçues") }}::float              as somme_quantites_recues,
        {{ adapter.quote("Nombre des transferts réceptionnés") }}::int        as nombre_transferts_receptionnes,
        {{ adapter.quote("annee") }}::int                                     as annee,
        row_number()
            over (
                partition by {{ adapter.quote("numéro de notification") }}
                order by
                    {{ adapter.quote("annee") }} desc,
                    {{ adapter.quote("inserted_at") }} desc nulls last
            )
        as rn
    from source
)

select
    numero_notification,
    type_dossier,
    etat_dossier,
    departement_dossier,
    nom_notifiant,
    numero_gistrid_notifiant,
    commune_notifiant,
    pays_notifiant,
    nom_producteur,
    commune_producteur,
    pays_producteur,
    siret_installation_traitement,
    nom_installation_traitement,
    numero_gistrid_installation_traitement,
    commune_installation_traitement,
    pays_installation_traitement,
    pays_transit,
    date_consentement_francais,
    date_autorisee_debut_transferts,
    date_autorisee_fin_transferts,
    code_d_r,
    code_cb,
    code_ced,
    code_ocde,
    code_y,
    code_h,
    caracteristiques_physiques,
    quantite_autorisee,
    unite,
    nombre_total_transferts_autorises,
    somme_quantites_recues,
    nombre_transferts_receptionnes,
    annee
from renamed
where rn = 1
