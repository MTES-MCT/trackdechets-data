{{
  config(
    materialized = 'table',
    indexes = [  {'columns': ['siret'] }]
    )
}}

with gerep_traiteurs as (
    select
        *,
        row_number() over (
            partition by code_etablissement
            order by
                annee desc
        )
    from
        {{ ref('gerep_traiteurs_2021') }}
),

gerep_producteurs as (
    select
        *,
        row_number() over (
            partition by code_etablissement
            order by
                annee desc
        )
    from
        {{ ref('gerep_producteurs_2021') }}
),

matching_manuel as (
    select
        code_aiot,
        siret_td
    from {{ ref('matching_td_georisques') }}
    where traite = 'oui'
)

select
    i.code_aiot,
    i.raison_sociale,
    i.num_siret                                            as siret_icpe,
    gt.siret                                               as siret_gerep_traiteurs,
    gp.siret                                               as siret_gerep_producteurs,
    mm.siret_td                                            as siret_matching_manuel,
    i.etat_activite,
    i.regime,
    i.longitude,
    i.latitude,
    i."adresse1",
    i."adresse2",
    i.code_postal,
    i."commune",
    i.code_insee,
    coalesce(mm.siret_td, i.num_siret, gt.siret, gp.siret) as siret
from
    {{ ref('installations') }} as i
left join gerep_traiteurs as gt
    on
        i.code_aiot = gt.code_etablissement and gt.row_number = 1
left join gerep_producteurs as gp
    on
        i.code_aiot = gp.code_etablissement and gp.row_number = 1
left join matching_manuel as mm on
    i.code_aiot = mm.code_aiot
