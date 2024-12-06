{{
  config(
    materialized = 'table',
     indexes=[{"columns":["numero_notification"], "unique":True},
    {"columns":["numero_gistrid_notifiant"]},
    {"columns":["numero_gistrid_installation_traitement"]},
    {"columns":["siret_notifiant"]},
    {"columns":["siret_installation_traitement"]}]
    )
}}

with enriched as (
    select
        numero_notification,
        annee,
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
        coalesce(
            o1.siret, o1.numero_enregistrement
        ) as siret_notifiant,
        coalesce(
            n.siret_installation_traitement, o2.siret, o2.numero_enregistrement
        ) as siret_installation_traitement
    from {{ ref('notifications') }} as n
    left join
        {{ ref('operateurs') }} as o1
        on n.numero_gistrid_notifiant = o1.numero_gistrid
    left join
        {{ ref('operateurs') }} as o2
        on n.numero_gistrid_installation_traitement = o2.numero_gistrid
)

select
    numero_notification,
    annee,
    type_dossier,
    etat_dossier,
    departement_dossier,
    nom_notifiant,
    numero_gistrid_notifiant,
    siret_notifiant,
    commune_notifiant,
    pays_notifiant,
    nom_producteur,
    commune_producteur,
    pays_producteur,
    nom_installation_traitement,
    numero_gistrid_installation_traitement,
    siret_installation_traitement,
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
    nombre_transferts_receptionnes
from enriched
