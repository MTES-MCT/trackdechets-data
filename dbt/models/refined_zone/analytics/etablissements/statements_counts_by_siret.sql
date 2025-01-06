{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'],
    'unique': True },],
    )
}}

with dnd_entrant_stats as (
    select
        etablissement_numero_identification as siret,
        count(
            distinct id
        )                                   as num_dnd_statements_as_destination,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                   as quantity_dnd_statements_as_destination,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                   as volume_dnd_statements_as_destination,
        array_agg(
            distinct code_traitement
        )                                   as dnd_processing_operations_as_destination
    from {{ ref('dnd_entrant') }}
    group by 1
),

dnd_sortant_stats as (
    select
        producteur_numero_identification as siret,
        count(
            distinct id
        )                                as num_dnd_statements_as_emitter,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                as quantity_dnd_statements_as_emitter,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                as volume_dnd_statements_as_emitter
    from {{ ref('dnd_sortant') }}
    group by 1
),

dnd_transporteur_stats as (
    select
        numero_indentification_transporteur as siret,
        count(
            distinct id
        )                                   as num_dnd_statements_as_transporteur,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                   as quantity_dnd_statements_as_transporteur,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                   as volume_dnd_statements_as_transporteur
    from {{ ref('dnd_entrant') }},
        unnest(
            {{ ref('dnd_entrant') }}.numeros_indentification_transporteurs
        ) as numero_indentification_transporteur
    group by 1
),

texs_entrant_stats as (
    select
        etablissement_numero_identification as siret,
        count(
            distinct id
        )                                   as num_texs_statements_as_destination,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                   as quantity_texs_statements_as_destination,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                   as volume_texs_statements_as_destination,
        array_agg(
            distinct code_traitement
        )                                   as texs_processing_operations_as_destination
    from {{ ref('texs_entrant') }}
    group by 1
),

texs_sortant_stats as (
    select
        producteur_numero_identification as siret,
        count(
            distinct id
        )                                as num_texs_statements_as_emitter,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                as quantity_texs_statements_as_emitter,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                as volume_texs_statements_as_emitter
    from {{ ref('texs_sortant') }}
    group by 1
),

texs_transporteur_stats as (
    select
        numero_indentification_transporteur as siret,
        count(
            distinct id
        )                                   as num_texs_statements_as_transporteur,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                   as quantity_texs_statements_as_transporteur,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                   as volume_texs_statements_as_transporteur
    from {{ ref('texs_entrant') }},
        unnest(
            {{ ref('texs_entrant') }}.numeros_indentification_transporteurs
        ) as numero_indentification_transporteur
    group by 1
),

ssd_stats as (
    select
        etablissement_numero_identification as siret,
        count(
            distinct id
        )                                   as num_ssd_statements_as_destination,
        sum(quantite) filter (
            where code_unite = 'T'
        )                                   as quantity_ssd_statements_as_destination,
        sum(quantite) filter (
            where code_unite = 'M3'
        )                                   as volume_ssd_statements_as_destination
    from {{ ref("sortie_statut_dechet") }}
    group by 1
)

select
    dnd_processing_operations_as_destination,
    texs_processing_operations_as_destination,
    coalesce(
        dnd1.siret,
        dnd2.siret,
        dnd3.siret,
        texs1.siret,
        texs2.siret,
        texs3.siret,
        ssd.siret
    ) as siret,
    coalesce(
        num_dnd_statements_as_destination, 0
    ) as num_dnd_statements_as_destination,
    coalesce(
        quantity_dnd_statements_as_destination, 0
    ) as quantity_dnd_statements_as_destination,
    coalesce(
        volume_dnd_statements_as_destination, 0
    ) as volume_dnd_statements_as_destination,
    coalesce(
        num_dnd_statements_as_emitter, 0
    ) as num_dnd_statements_as_emitter,
    coalesce(
        quantity_dnd_statements_as_emitter, 0
    ) as quantity_dnd_statements_as_emitter,
    coalesce(
        volume_dnd_statements_as_emitter, 0
    ) as volume_dnd_statements_as_emitter,
    coalesce(
        num_dnd_statements_as_transporteur, 0
    ) as num_dnd_statements_as_transporteur,
    coalesce(
        quantity_dnd_statements_as_transporteur, 0
    ) as quantity_dnd_statements_as_transporteur,
    coalesce(
        volume_dnd_statements_as_transporteur, 0
    ) as volume_dnd_statements_as_transporteur,
    coalesce(
        num_texs_statements_as_destination, 0
    ) as num_texs_statements_as_destination,
    coalesce(
        quantity_texs_statements_as_destination, 0
    ) as quantity_texs_statements_as_destination,
    coalesce(
        volume_texs_statements_as_destination, 0
    ) as volume_texs_statements_as_destination,
    coalesce(
        num_texs_statements_as_emitter, 0
    ) as num_texs_statements_as_emitter,
    coalesce(
        quantity_texs_statements_as_emitter, 0
    ) as quantity_texs_statements_as_emitter,
    coalesce(
        volume_texs_statements_as_emitter, 0
    ) as volume_texs_statements_as_emitter,
    coalesce(
        num_texs_statements_as_transporteur, 0
    ) as num_texs_statements_as_transporteur,
    coalesce(
        quantity_texs_statements_as_transporteur, 0
    ) as quantity_texs_statements_as_transporteur,
    coalesce(
        volume_texs_statements_as_transporteur, 0
    ) as volume_texs_statements_as_transporteur,
    coalesce(
        num_ssd_statements_as_destination, 0
    ) as num_ssd_statements_as_destination,
    coalesce(
        quantity_ssd_statements_as_destination, 0
    ) as quantity_ssd_statements_as_destination,
    coalesce(
        volume_ssd_statements_as_destination, 0
    ) as volume_ssd_statements_as_destination
from dnd_entrant_stats as dnd1
full outer join dnd_sortant_stats as dnd2 on dnd1.siret = dnd2.siret
full outer join
    dnd_transporteur_stats as dnd3
    on coalesce(dnd1.siret, dnd2.siret) = dnd3.siret
full outer join
    texs_entrant_stats as texs1
    on coalesce(dnd1.siret, dnd2.siret, dnd3.siret) = texs1.siret
full outer join
    texs_sortant_stats as texs2
    on coalesce(dnd1.siret, dnd2.siret, dnd3.siret, texs1.siret) = texs2.siret
full outer join
    texs_transporteur_stats as texs3
    on
        coalesce(dnd1.siret, dnd2.siret, dnd3.siret, texs1.siret, texs2.siret)
        = texs3.siret
full outer join
    ssd_stats as ssd
    on
        coalesce(
            dnd1.siret,
            dnd2.siret,
            dnd3.siret,
            texs1.siret,
            texs2.siret,
            texs3.siret
        )
        = ssd.siret
