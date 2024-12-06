{{
  config(
    materialized = 'table',
    )
}}
with destinataires as (
    select
        destination_company_siret as siret,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSDD'
        )                         as num_bsdd,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSDD'
        )                         as quantite_traitee_bsdd,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSDA'
        )                         as num_bsda,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSDA'
        )                         as quantite_traitee_bsda,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSFF'
        )                         as num_bsff,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSFF'
        )                         as quantite_traitee_bsff,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSDASRI'
        )                         as num_bsdasri,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSDASRI'
        )                         as quantite_traitee_bsdasri,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSVHU'
        )                         as num_bsvhu,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSVHU'
        )                         as quantite_traitee_bsvhu
    from
        {{ ref('bordereaux_enriched') }} as be
    where
        destination_region = 11
        and be.processed_at >= '2023-01-01'
    group by
        destination_company_siret
),

emetteurs as (
    select
        be.emitter_company_siret as siret,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSDD'
        )                        as num_bsdd_envoyes,
        sum(be.quantity_received) filter (
            where
            "_bs_type" = 'BSDD'
        )                        as quantite_envoyee_bsdd,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSDA'
        )                        as num_bsda_envoyes,
        sum(quantity_received) filter (
            where
            "_bs_type" = 'BSDA'
        )                        as quantite_envoyee_bsda,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSFF'
        )                        as num_bsff_envoyes,
        sum(be.quantity_received) filter (
            where
            "_bs_type" = 'BSFF'
        )                        as quantite_envoyee_bsff,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSDASRI'
        )                        as num_bsdasri_envoyes,
        sum(be.quantity_received) filter (
            where
            "_bs_type" = 'BSDASRI'
        )                        as quantite_envoyee_bsdasri,
        count(distinct id) filter (
            where
            "_bs_type" = 'BSVHU'
        )                        as num_bsvhu_envoyes,
        sum(be.quantity_received) filter (
            where
            "_bs_type" = 'BSVHU'
        )                        as quantite_envoyee_bsvhu
    from
        {{ ref('bordereaux_enriched') }} as be
    where
        be.taken_over_at >= '2023-01-01'
        and be.emitter_region = 11
    group by
        siret
),

grouped as (
    select
        num_bsdd,
        quantite_traitee_bsdd,
        num_bsda,
        quantite_traitee_bsda,
        num_bsff,
        quantite_traitee_bsff,
        num_bsdasri,
        quantite_traitee_bsdasri,
        num_bsdd_envoyes,
        quantite_envoyee_bsdd,
        num_bsda_envoyes,
        quantite_envoyee_bsda,
        num_bsff_envoyes,
        quantite_envoyee_bsff,
        num_bsdasri_envoyes,
        quantite_envoyee_bsdasri,
        num_bsvhu_envoyes,
        quantite_envoyee_bsvhu,
        coalesce(
            d.siret,
            e.siret
        ) as siret
    from
        destinataires as d
    full outer join emetteurs as e
        on
            d.siret = e.siret
)

select
    g.siret,
    max(
        coalesce(
            c.name,
            ir.raison_sociale,
            coalesce(
                se.enseigne_1_etablissement,
                se.enseigne_2_etablissement,
                se.enseigne_3_etablissement
            )
        )
    )             as nom_etablissement,
    max(
        coalesce(
            c.address,
            coalesce(numero_voie_etablissement || ' ', '')
            || coalesce(type_voie_etablissement || ' ', '')
            || coalesce(libelle_voie_etablissement || ' ', '')
            || coalesce(code_postal_etablissement || ' ', '')
            || coalesce(libelle_commune_etablissement, '')
        )
    )             as adresse_etablissement,
    max(
        c.gerep_id
    )             as numero_gerep,
    max(
        c.id
    ) is not null as inscrit_td,
    max(
        ir.code_aiot
    ) is not null as inscrit_gun,
    array_to_string(
        array_agg(distinct ir.code_aiot), ', '
    )             as codes_installations,
    array_to_string(
        array_agg(distinct ir.rubrique), ', '
    )             as codes_rubriques,
    max(
        num_bsdd
    )             as num_bsdd_traites,
    max(
        quantite_traitee_bsdd
    )             as quantite_traitee_bsdd,
    max(
        num_bsda
    )             as num_bsda_traites,
    max(
        quantite_traitee_bsda
    )             as quantite_traitee_bsda,
    max(
        num_bsff
    )             as num_bsff_traites,
    max(
        quantite_traitee_bsff
    )             as quantite_traitee_bsff,
    max(
        num_bsdasri
    )             as num_bsdasri_traites,
    max(
        quantite_traitee_bsdasri
    )             as quantite_traitee_bsdasri,
    max(
        num_bsdd_envoyes
    )             as num_bsdd_envoyes,
    max(
        quantite_envoyee_bsdd
    )             as quantite_envoyee_bsdd,
    max(
        num_bsda_envoyes
    )             as num_bsda_envoyes,
    max(
        quantite_envoyee_bsda
    )             as quantite_envoyee_bsda,
    max(
        num_bsff_envoyes
    )             as num_bsff_envoyes,
    max(
        quantite_envoyee_bsff
    )             as quantite_envoyee_bsff,
    max(
        num_bsdasri_envoyes
    )             as num_bsdasri_envoyes,
    max(
        quantite_envoyee_bsdasri
    )             as quantite_envoyee_bsdasri,
    max(
        num_bsvhu_envoyes
    )             as num_bsvhu_envoyes,
    max(
        quantite_envoyee_bsvhu
    )             as quantite_envoyee_bsvhu
from
    grouped as g
left join {{ ref('company') }} as c on g.siret = c.siret
left join {{ ref('installations_rubriques_2024') }} as ir
    on
        g.siret = ir.siret
left join {{ ref('stock_etablissement') }} as se on g.siret = se.siret
group by
    g.siret
