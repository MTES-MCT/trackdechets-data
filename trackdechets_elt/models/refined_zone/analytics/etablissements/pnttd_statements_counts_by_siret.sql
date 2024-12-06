{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'],
    'unique': True },],
) }}

with export_stats as (
    select
        siret_notifiant                     as siret,
        count(distinct numero_notification) as num_statements_as_emitter,
        sum(somme_quantites_recues)         as quantity_as_emitter
    from {{ ref("notifications_enriched") }}
    where type_dossier = 'Exportation'
    group by 1
),

imports_stats as (
    select
        siret_installation_traitement       as siret,
        count(distinct numero_notification) as num_statements_as_destination,
        sum(somme_quantites_recues)         as quantity_as_destination
    from {{ ref("notifications_enriched") }}
    where type_dossier = 'Importation'
    group by 1
)

select
    coalesce(imports.siret, exports.siret)     as siret,
    coalesce(num_statements_as_emitter, 0)     as num_statements_as_emitter,
    coalesce(quantity_as_emitter, 0)           as quantity_as_emitter,
    coalesce(num_statements_as_destination, 0) as num_statements_as_destination,
    coalesce(quantity_as_destination, 0)       as quantity_as_destination
from export_stats as exports
full outer join imports_stats as imports on exports.siret = imports.siret
