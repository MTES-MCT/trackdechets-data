{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'],
    'unique': True },],
) }}



with bsdd_transporter_counts as (
    select
        transporter_company_siret as "siret",
        COUNT(id) filter (
            where {{ dangerous_waste_filter('bsdd') }}
        )                         as num_bsdd_as_transporter,
        COUNT(id) filter (
            where not {{ dangerous_waste_filter('bsdd') }}
        )                         as num_bsdnd_as_transporter,
        SUM(quantity_received) filter (
            where
            {{ dangerous_waste_filter('bsdd') }}
        )                         as quantity_bsdd_as_transporter,
        SUM(quantity_received) filter (
            where
            not {{ dangerous_waste_filter('bsdd') }}
        )                         as quantity_bsdnd_as_transporter,
        MAX(
            created_at
        )                         as last_bordereau_created_at_as_transporter_bsdd,
        ARRAY_AGG(
            distinct processing_operation_done
        )                         as processing_operations_as_transporter_bsdd
    from
        {{ ref('bsdd_transporter_enriched') }}
    group by
        transporter_company_siret
),

bsda_transporter_counts as (
    select
        transporter_company_siret as "siret",
        COUNT(
            id
        )                         as num_bsda_as_transporter,
        SUM(
            destination_reception_weight
        )                         as quantity_bsda_as_transporter,
        MAX(
            created_at
        )                         as last_bordereau_created_at_as_transporter_bsda,
        ARRAY_AGG(
            distinct destination_operation_code
        )                         as processing_operations_as_transporter_bsda
    from
        {{ ref('bsda_transporter_enriched') }}
    group by
        transporter_company_siret
),

bsff_transporter_counts as (
    select
        transporter_company_siret as "siret",
        COUNT(id)                 as num_bsff_as_transporter,
        SUM(acceptation_weight)   as quantity_bsff_as_transporter,
        MAX(
            created_at
        )                         as last_bordereau_created_at_as_transporter_bsff,
        ARRAY_AGG(
            distinct operation_code
        )                         as processing_operations_as_transporter_bsff
    from
        {{ ref('bsff_transporter_enriched') }}
    group by
        transporter_company_siret
),


transporter_counts_legacy as (
    select
        transporter_company_siret as "siret",
        COUNT(id) filter (
            where
            _bs_type = 'BSDASRI'
        )                         as num_bsdasri_as_transporter,
        COUNT(id) filter (
            where
            _bs_type = 'BSVHU'
        )                         as num_bsvhu_as_transporter,
        SUM(quantity_received) filter (
            where
            _bs_type = 'BSDASRI'
        )                         as quantity_bsdasri_as_transporter,
        SUM(quantity_received) filter (
            where
            _bs_type = 'BSVHU'
        )                         as quantity_bsvhu_as_transporter,
        MAX(
            created_at
        )                         as last_bordereau_created_at_as_transporter,
        ARRAY_AGG(
            distinct processing_operation
        )                         as processing_operations_as_transporter
    from
        {{ ref('bordereaux_enriched') }}
    group by
        transporter_company_siret
),

grouped as (
    select
        COALESCE(
            bsdd_tc.siret,
            bsda_tc.siret,
            bsff_tc.siret,
            tc.siret
        )                                          as siret,
        GREATEST(
            last_bordereau_created_at_as_transporter,
            last_bordereau_created_at_as_transporter_bsdd,
            last_bordereau_created_at_as_transporter_bsda,
            last_bordereau_created_at_as_transporter_bsff
        )
        as last_bordereau_created_at_as_transporter,
        COALESCE(
            bsdd_tc.num_bsdd_as_transporter,
            0
        )                                          as num_bsdd_as_transporter,
        COALESCE(
            bsdd_tc.num_bsdnd_as_transporter,
            0
        )                                          as num_bsdnd_as_transporter,
        COALESCE(
            bsda_tc.num_bsda_as_transporter,
            0
        )                                          as num_bsda_as_transporter,
        COALESCE(
            bsff_tc.num_bsff_as_transporter,
            0
        )                                          as num_bsff_as_transporter,
        COALESCE(
            tc.num_bsdasri_as_transporter,
            0
        )                                          as num_bsdasri_as_transporter,
        COALESCE(
            tc.num_bsvhu_as_transporter,
            0
        )                                          as num_bsvhu_as_transporter,
        COALESCE(
            bsdd_tc.quantity_bsdd_as_transporter,
            0
        )                                          as quantity_bsdd_as_transporter,
        COALESCE(
            bsdd_tc.quantity_bsdnd_as_transporter,
            0
        )                                          as quantity_bsdnd_as_transporter,
        COALESCE(
            bsda_tc.quantity_bsda_as_transporter,
            0
        )                                          as quantity_bsda_as_transporter,
        COALESCE(
            bsff_tc.quantity_bsff_as_transporter,
            0
        )                                          as quantity_bsff_as_transporter,
        COALESCE(
            tc.quantity_bsdasri_as_transporter,
            0
        )                                          as quantity_bsdasri_as_transporter,
        COALESCE(
            tc.quantity_bsvhu_as_transporter,
            0
        )                                          as quantity_bsvhu_as_transporter,
        bsdd_tc.processing_operations_as_transporter_bsdd
        || bsda_tc.processing_operations_as_transporter_bsda
        || bsff_tc.processing_operations_as_transporter_bsff
        || tc.processing_operations_as_transporter as processing_operations_as_transporter
    from
        bsdd_transporter_counts as bsdd_tc
    full
    outer join
        bsda_transporter_counts as bsda_tc
        on
            bsdd_tc.siret = bsda_tc.siret
    full
    outer join
        bsff_transporter_counts as bsff_tc
        on
            COALESCE(bsdd_tc.siret, bsda_tc.siret)
            = bsff_tc.siret
    full
    outer join
        transporter_counts_legacy as tc
        on
            COALESCE(bsdd_tc.siret, bsda_tc.siret, bsff_tc.siret)
            = tc.siret
)

-- Deduplication of processing operations done

select
    grouped.siret,
    MAX(
        last_bordereau_created_at_as_transporter
    ) as last_bordereau_created_at_as_transporter,
    MAX(
        num_bsdd_as_transporter
    ) as num_bsdd_as_transporter,
    MAX(
        num_bsdnd_as_transporter
    ) as num_bsdnd_as_transporter,
    MAX(
        num_bsda_as_transporter
    ) as num_bsda_as_transporter,
    MAX(
        num_bsff_as_transporter
    ) as num_bsff_as_transporter,
    MAX(
        num_bsdasri_as_transporter
    ) as num_bsdasri_as_transporter,
    MAX(
        num_bsvhu_as_transporter
    ) as num_bsvhu_as_transporter,
    MAX(
        quantity_bsdd_as_transporter
    ) as quantity_bsdd_as_transporter,
    MAX(
        quantity_bsdnd_as_transporter
    ) as quantity_bsdnd_as_transporter,
    MAX(
        quantity_bsda_as_transporter
    ) as quantity_bsda_as_transporter,
    MAX(
        quantity_bsff_as_transporter
    ) as quantity_bsff_as_transporter,
    MAX(
        quantity_bsdasri_as_transporter
    ) as quantity_bsdasri_as_transporter,
    MAX(
        quantity_bsvhu_as_transporter
    ) as quantity_bsvhu_as_transporter,
    ARRAY_AGG(
        distinct processing_operation_as_transporter
    ) as processing_operations_as_transporter
from grouped,
    UNNEST(
        grouped.processing_operations_as_transporter
    ) as processing_operation_as_transporter
group by 1
