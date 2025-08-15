{{ config(
    materialized = 'table',
) }}

with bordereaux as (
    select
        _bs_type,
        id,
        created_at,
        emitter_company_siret,
        destination_company_siret,
        worker_company_siret,
        accepted_quantity_packagings,
        quantity_received,
        waste_code,
        waste_pop,
        waste_is_dangerous,
        processing_operation
    from
        {{ ref('bordereaux_enriched') }}
    where
        status not in {{ get_non_final_status_bordereaux() }}
        and not is_draft
),

emitter_counts as (
    select
        emitter_company_siret                             as siret,
        countIf(
            id,
            _bs_type = 'BSDD'
            and ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )
            as num_bsdd_as_emitter,
        countIf(
            id,
            _bs_type = 'BSDD'
            and not ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )
            as num_bsdnd_as_emitter,
        countIf(
            id,
            _bs_type = 'BSDA'
        )
            as num_bsda_as_emitter,
        countIf(
            id,
            _bs_type = 'BSFF'
        )
            as num_bsff_as_emitter,
        countIf(
            id,
            _bs_type = 'BSDASRI'
        )
            as num_bsdasri_as_emitter,
        countIf(
            id,
            _bs_type = 'BSVHU'
        )
            as num_bsvhu_as_emitter,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDD'
            and ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )
            as quantity_bsdd_as_emitter,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDD'
            and not ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )
            as quantity_bsdnd_as_emitter,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDA'
        )
            as quantity_bsda_as_emitter,
        sUM(accepted_quantity_packagings) filter (
            where
            _bs_type = 'BSFF'
        )
            as quantity_bsff_as_emitter,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDASRI'
        )
            as quantity_bsdasri_as_emitter,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSVHU'
        )
            as quantity_bsvhu_as_emitter,
        mAX(
            created_at
        )
            as last_bordereau_created_at_as_emitter,
        aRRAY_aGG(
            distinct processing_operation
        ) filter (where processing_operation is not null)
            as processing_operations_as_emitter
    from
        bordereaux
    group by
        emitter_company_siret
),

transporter_counts as (
    select
        siret,
        last_bordereau_created_at_as_transporter,
        num_bsdd_as_transporter,
        num_bsdnd_as_transporter,
        num_bsda_as_transporter,
        num_bsff_as_transporter,
        num_bsdasri_as_transporter,
        num_bsvhu_as_transporter,
        quantity_bsdd_as_transporter,
        quantity_bsdnd_as_transporter,
        quantity_bsda_as_transporter,
        quantity_bsff_as_transporter,
        quantity_bsdasri_as_transporter,
        quantity_bsvhu_as_transporter,
        processing_operations_as_transporter
    from
        {{ ref('transporters_bordereaux_counts_by_siret') }}
),

destination_counts as (
    select
        destination_company_siret               as siret,
        countIf(
            id,
            _bs_type = 'BSDD'
            and ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                                       as num_bsdd_as_destination,
        countIf(
            id,
            _bs_type = 'BSDD'
            and not ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                                       as num_bsdnd_as_destination,
        countIf(
            id,
            _bs_type = 'BSDA'
        )                                       as num_bsda_as_destination,
        countIf(
            id,
            _bs_type = 'BSFF'
        )                                       as num_bsff_as_destination,
        countIf(
            id,
            _bs_type = 'BSDASRI'
        )                                       as num_bsdasri_as_destination,
        countIf(
            id,
            _bs_type = 'BSVHU'
        )                                       as num_bsvhu_as_destination,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDD'
            and ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                                       as quantity_bsdd_as_destination,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDD'
            and not ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )
            as quantity_bsdnd_as_destination,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDA'
        )                                       as quantity_bsda_as_destination,
        sUM(accepted_quantity_packagings) filter (
            where
            _bs_type = 'BSFF'
        )                                       as quantity_bsff_as_destination,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDASRI'
        )
            as quantity_bsdasri_as_destination,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSVHU'
        )
            as quantity_bsvhu_as_destination,
        mAX(
            created_at
        )
            as last_bordereau_created_at_as_destination,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSDD'
            and ({{ dangerous_waste_filter('bordereaux_enriched') }})
            and processing_operation is not null
        )
            as processing_operations_as_destination_bsdd,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSDD'
            and not ({{ dangerous_waste_filter('bordereaux_enriched') }})
            and processing_operation is not null
        )
            as processing_operations_as_destination_bsdnd,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSDA'
            and processing_operation is not null
        )
            as processing_operations_as_destination_bsda,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSFF'
            and processing_operation is not null
        )
            as processing_operations_as_destination_bsff,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSDASRI'
            and processing_operation is not null
        )
            as processing_operations_as_destination_bsdasri,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSVHU'
            and processing_operation is not null
        )
            as processing_operations_as_destination_bsvhu,
        groupArray(
            distinct waste_code
        ) filter (where waste_code is not null) as waste_codes_as_destination
    from
        bordereaux
    group by
        destination_company_siret
),

worker_counts as (
    select
        worker_company_siret as siret,
        countIf(
            id,
            _bs_type = 'BSDA'
        )                    as num_bsda_as_worker,
        sUM(quantity_received) filter (
            where
            _bs_type = 'BSDA'
        )                    as quantity_bsda_as_worker,
        mAX(
            created_at
        )                    as last_bordereau_created_at_as_worker,
        groupArray(
            distinct processing_operation
        ) filter (
            where
            _bs_type = 'BSDA'
            and processing_operation is not null
        )                    as processing_operations_as_worker
    from
        bordereaux
    group by
        worker_company_siret
),

full_ as (
    select
        last_bordereau_created_at_as_emitter,
        last_bordereau_created_at_as_transporter,
        last_bordereau_created_at_as_destination,
        last_bordereau_created_at_as_worker,
        processing_operations_as_emitter,
        processing_operations_as_transporter,
        processing_operations_as_destination_bsdd,
        processing_operations_as_destination_bsdnd,
        processing_operations_as_destination_bsda,
        processing_operations_as_destination_bsff,
        processing_operations_as_destination_bsdasri,
        processing_operations_as_destination_bsvhu,
        processing_operations_as_worker,
        waste_codes_as_destination,
        cOALESCE(
            emitter_counts.siret,
            transporter_counts.siret,
            destination_counts.siret,
            c.siret
        ) as siret,
        cOALESCE(
            emitter_counts.num_bsdd_as_emitter,
            0
        ) as num_bsdd_as_emitter,
        cOALESCE(
            emitter_counts.num_bsdnd_as_emitter,
            0
        ) as num_bsdnd_as_emitter,
        cOALESCE(
            emitter_counts.num_bsda_as_emitter,
            0
        ) as num_bsda_as_emitter,
        cOALESCE(
            emitter_counts.num_bsff_as_emitter,
            0
        ) as num_bsff_as_emitter,
        cOALESCE(
            emitter_counts.num_bsdasri_as_emitter,
            0
        ) as num_bsdasri_as_emitter,
        cOALESCE(
            emitter_counts.num_bsvhu_as_emitter,
            0
        ) as num_bsvhu_as_emitter,
        cOALESCE(
            emitter_counts.quantity_bsdd_as_emitter,
            0
        ) as quantity_bsdd_as_emitter,
        cOALESCE(
            emitter_counts.quantity_bsdnd_as_emitter,
            0
        ) as quantity_bsdnd_as_emitter,
        cOALESCE(
            emitter_counts.quantity_bsda_as_emitter,
            0
        ) as quantity_bsda_as_emitter,
        cOALESCE(
            emitter_counts.quantity_bsff_as_emitter,
            0
        ) as quantity_bsff_as_emitter,
        cOALESCE(
            emitter_counts.quantity_bsdasri_as_emitter,
            0
        ) as quantity_bsdasri_as_emitter,
        cOALESCE(
            emitter_counts.quantity_bsvhu_as_emitter,
            0
        ) as quantity_bsvhu_as_emitter,
        cOALESCE(
            transporter_counts.num_bsdnd_as_transporter,
            0
        ) as num_bsdnd_as_transporter,
        cOALESCE(
            transporter_counts.num_bsdd_as_transporter,
            0
        ) as num_bsdd_as_transporter,
        cOALESCE(
            transporter_counts.num_bsda_as_transporter,
            0
        ) as num_bsda_as_transporter,
        cOALESCE(
            transporter_counts.num_bsff_as_transporter,
            0
        ) as num_bsff_as_transporter,
        cOALESCE(
            transporter_counts.num_bsdasri_as_transporter,
            0
        ) as num_bsdasri_as_transporter,
        cOALESCE(
            transporter_counts.num_bsvhu_as_transporter,
            0
        ) as num_bsvhu_as_transporter,
        cOALESCE(
            transporter_counts.quantity_bsdd_as_transporter,
            0
        ) as quantity_bsdd_as_transporter,
        cOALESCE(
            transporter_counts.quantity_bsdnd_as_transporter,
            0
        ) as quantity_bsdnd_as_transporter,
        cOALESCE(
            transporter_counts.quantity_bsda_as_transporter,
            0
        ) as quantity_bsda_as_transporter,
        cOALESCE(
            transporter_counts.quantity_bsff_as_transporter,
            0
        ) as quantity_bsff_as_transporter,
        cOALESCE(
            transporter_counts.quantity_bsdasri_as_transporter,
            0
        ) as quantity_bsdasri_as_transporter,
        cOALESCE(
            transporter_counts.quantity_bsvhu_as_transporter,
            0
        ) as quantity_bsvhu_as_transporter,
        cOALESCE(
            destination_counts.num_bsdd_as_destination, 0
        ) as num_bsdd_as_destination,
        cOALESCE(
            destination_counts.num_bsdnd_as_destination, 0
        ) as num_bsdnd_as_destination,
        cOALESCE(
            destination_counts.num_bsda_as_destination, 0
        ) as num_bsda_as_destination,
        cOALESCE(
            destination_counts.num_bsff_as_destination, 0
        ) as num_bsff_as_destination,
        cOALESCE(
            destination_counts.num_bsdasri_as_destination, 0
        ) as num_bsdasri_as_destination,
        cOALESCE(
            destination_counts.num_bsvhu_as_destination, 0
        ) as num_bsvhu_as_destination,
        cOALESCE(
            worker_counts.num_bsda_as_worker, 0
        ) as num_bsda_as_worker,
        cOALESCE(
            destination_counts.quantity_bsdd_as_destination,
            0
        ) as quantity_bsdd_as_destination,
        cOALESCE(
            destination_counts.quantity_bsdnd_as_destination,
            0
        ) as quantity_bsdnd_as_destination,
        cOALESCE(
            destination_counts.quantity_bsda_as_destination,
            0
        ) as quantity_bsda_as_destination,
        cOALESCE(
            destination_counts.quantity_bsff_as_destination,
            0
        ) as quantity_bsff_as_destination,
        cOALESCE(
            destination_counts.quantity_bsdasri_as_destination,
            0
        ) as quantity_bsdasri_as_destination,
        cOALESCE(
            destination_counts.quantity_bsvhu_as_destination,
            0
        ) as quantity_bsvhu_as_destination
    from
        emitter_counts
    full
    outer join
        transporter_counts
        on
            emitter_counts.siret = transporter_counts.siret
    full
    outer join
        destination_counts
        on
            cOALESCE(emitter_counts.siret, transporter_counts.siret)
            = destination_counts.siret
    full
    outer join
        worker_counts
        on
            cOALESCE(
                emitter_counts.siret,
                transporter_counts.siret,
                destination_counts.siret
            )
            = worker_counts.siret
    full outer join
        {{ ref('company') }} as c
        on
            cOALESCE(
                emitter_counts.siret,
                transporter_counts.siret,
                destination_counts.siret,
                worker_counts.siret
            )
            = c.siret
)

select
    siret,
    last_bordereau_created_at_as_emitter,
    last_bordereau_created_at_as_transporter,
    last_bordereau_created_at_as_destination,
    last_bordereau_created_at_as_worker,
    processing_operations_as_emitter,
    processing_operations_as_transporter,
    processing_operations_as_destination_bsdd,
    processing_operations_as_destination_bsdnd,
    processing_operations_as_destination_bsda,
    processing_operations_as_destination_bsff,
    processing_operations_as_destination_bsdasri,
    processing_operations_as_destination_bsvhu,
    processing_operations_as_worker,
    waste_codes_as_destination,
    num_bsdd_as_emitter,
    num_bsdnd_as_emitter,
    num_bsda_as_emitter,
    num_bsff_as_emitter,
    num_bsdasri_as_emitter,
    num_bsvhu_as_emitter,
    quantity_bsdd_as_emitter,
    quantity_bsdnd_as_emitter,
    quantity_bsda_as_emitter,
    quantity_bsff_as_emitter,
    quantity_bsdasri_as_emitter,
    quantity_bsvhu_as_emitter,
    num_bsdnd_as_transporter,
    num_bsdd_as_transporter,
    num_bsda_as_transporter,
    num_bsff_as_transporter,
    num_bsdasri_as_transporter,
    num_bsvhu_as_transporter,
    num_bsda_as_worker,
    quantity_bsdd_as_transporter,
    quantity_bsdnd_as_transporter,
    quantity_bsda_as_transporter,
    quantity_bsff_as_transporter,
    quantity_bsdasri_as_transporter,
    quantity_bsvhu_as_transporter,
    num_bsdd_as_destination,
    num_bsdnd_as_destination,
    num_bsda_as_destination,
    num_bsff_as_destination,
    num_bsdasri_as_destination,
    num_bsvhu_as_destination,
    quantity_bsdd_as_destination,
    quantity_bsdnd_as_destination,
    quantity_bsda_as_destination,
    quantity_bsff_as_destination,
    quantity_bsdasri_as_destination,
    quantity_bsvhu_as_destination,
    gREATEST(
        last_bordereau_created_at_as_emitter,
        last_bordereau_created_at_as_transporter,
        last_bordereau_created_at_as_destination,
        last_bordereau_created_at_as_worker
    )                          as last_bordereau_created_at,
    num_bsdd_as_emitter
    + num_bsdnd_as_emitter
    + num_bsda_as_emitter
    + num_bsff_as_emitter
    + num_bsdasri_as_emitter
    + num_bsvhu_as_emitter
    + num_bsdd_as_transporter
    + num_bsdnd_as_transporter
    + num_bsda_as_transporter
    + num_bsff_as_transporter
    + num_bsdasri_as_transporter
    + num_bsvhu_as_transporter
    + num_bsdd_as_destination
    + num_bsdnd_as_destination
    + num_bsda_as_destination
    + num_bsff_as_destination
    + num_bsdasri_as_destination
    + num_bsvhu_as_destination as total_mentions_bordereaux
from
    full_
