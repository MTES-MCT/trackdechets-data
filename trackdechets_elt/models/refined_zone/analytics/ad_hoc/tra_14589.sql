{{
  config(
    materialized = 'table',
    )
}}


with emetteurs as (
    select
        "_bs_type",
        substring(emitter_company_siret for 9) as siren,
        emitter_company_siret                  as siret,
        count(*)                               as num_emetteur
    from {{ ref('bordereaux_enriched') }} as be
    where be.received_at >= now() - interval '1 year'
    group by 1, 2, 3
    order by 3 desc
),

-- To avoid having linked companies (same SIREN)
emetteurs_rankes as (
    select
        emetteurs.*,
        row_number()
            over (partition by "_bs_type", siren order by num_emetteur desc)
        as rang
    from emetteurs
),


transporteurs as (
-- Gestion du multimodal
    select
        "_bs_type",
        substring(coalesce(
            be.transporter_company_siret,
            bsddt.transporter_company_siret,
            bsdat.transporter_company_siret,
            bsfft.transporter_company_siret
        ) for 9) as siren,
        coalesce(
            be.transporter_company_siret,
            bsddt.transporter_company_siret,
            bsdat.transporter_company_siret,
            bsfft.transporter_company_siret
        )        as siret,
        count(
            distinct be.id
        )        as num_transporteur
    from {{ ref('bordereaux_enriched') }} as be
    left join
        {{ ref('bsdd_transporter') }} as bsddt
        on be.id = bsddt.form_id and be."_bs_type" = 'BSDD'
    left join
        {{ ref('bsda_transporter') }} as bsdat
        on be.id = bsdat.bsda_id and be."_bs_type" = 'BSDA'
    left join
        {{ ref('bsff_transporter') }} as bsfft
        on be.id = bsfft.bsff_id and be."_bs_type" = 'BSFF'
    where be.received_at >= now() - interval '1 year'
    group by 1, 2, 3
    order by 3 desc
),

-- To avoid having linked companies (same SIREN)
transporteurs_rankes as (
    select
        transporteurs.*,
        row_number()
            over (partition by "_bs_type", siren order by num_transporteur desc)
        as rang
    from transporteurs
),


destinataires as (
    select
        "_bs_type",
        substring(destination_company_siret for 9) as siren,
        destination_company_siret                  as siret,
        count(*)                                   as num_destinataire
    from {{ ref('bordereaux_enriched') }} as be
    where be.received_at >= now() - interval '1 year'
    group by 1, 2, 3
    order by 3 desc
),

-- To avoid having linked companies (same SIREN)
destinataires_rankes as (
    select
        destinataires.*,
        row_number()
            over (partition by "_bs_type", siren order by num_destinataire desc)
        as rang
    from destinataires
),



all_stats as (
    select
        coalesce(
            e."_bs_type", t."_bs_type", d."_bs_type"
        )                               as "_bs_type",
        coalesce(
            e.siren, t.siren, d.siren
        )                               as siren,
        coalesce(
            e.siret, t.siret, d.siret
        )                               as siret,
        num_emetteur,
        num_transporteur,
        num_destinataire,
        coalesce(num_emetteur, 0)
        + coalesce(num_transporteur, 0)
        + coalesce(num_destinataire, 0) as total
    from emetteurs_rankes as e
    full outer join
        transporteurs_rankes as t
        on e.siret = t.siret and e."_bs_type" = t."_bs_type"
    full outer join
        destinataires_rankes as d
        on
            coalesce(e.siret, t.siret) = d.siret
            and coalesce(e."_bs_type", t."_bs_type") = d."_bs_type"
    where
        coalesce(e.siret, t.siret, d.siret) is not null
        and coalesce(e.siret, t.siret, d.siret) != ''
        and (e.rang = 1 or e.rang is null)
        and (t.rang = 1 or t.rang is null)
        and (d.rang = 1 or d.rang is null)
    order by 1, 6 desc
),

--- Sélection de cohortes indépendantes pour chaque type de bordereaux (TOP 100 SIRETs en nombre de mentions)
top_bsdd as (
    select *
    from all_stats
    where "_bs_type" = 'BSDD'
    order by total desc
    limit 100
),

top_bsda as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSDA'
            and siren not in (select siren from top_bsdd)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsdd
),

top_bsdasri as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSDASRI'
            and siren not in (select siren from top_bsda)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsda
),

top_bsff as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSFF'
            and siren not in (select siren from top_bsdasri)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsdasri
),

top_bsvhu as (
    (
        select *
        from all_stats where
            "_bs_type" = 'BSVHU'
            and siren not in (select siren from top_bsff)
        order by total desc
        limit 100
    )
    union all
    select *
    from top_bsff
)

select * from top_bsvhu
