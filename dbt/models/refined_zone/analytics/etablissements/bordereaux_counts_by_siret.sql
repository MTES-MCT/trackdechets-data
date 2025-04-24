{{ config(
    materialized = 'table',
) }}

WITH emitter_counts AS (
    SELECT
        emitter_company_siret AS siret,
        countIf(id,
            _bs_type = 'BSDD'
            AND ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                     AS num_bsdd_as_emitter,
        countIf(id,
            _bs_type = 'BSDD'
            AND NOT ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                     AS num_bsdnd_as_emitter,
        countIf(id,
            _bs_type = 'BSDA'
        )                     AS num_bsda_as_emitter,
        countIf(id,
            _bs_type = 'BSFF'
        )                     AS num_bsff_as_emitter,
        countIf(id,
            _bs_type = 'BSDASRI'
        )                     AS num_bsdasri_as_emitter,
        countIf(id,
            _bs_type = 'BSVHU'
        )                     AS num_bsvhu_as_emitter,
        SUM(quantity_received) FILTER (
            where
            _bs_type = 'BSDD'
            AND ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                     AS quantity_bsdd_as_emitter,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDD'
            AND NOT ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                     AS quantity_bsdnd_as_emitter,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                     AS quantity_bsda_as_emitter,
        SUM(accepted_quantity_packagings) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                     AS quantity_bsff_as_emitter,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                     AS quantity_bsdasri_as_emitter,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                     AS quantity_bsvhu_as_emitter,
        MAX(
            created_at
        )                     AS last_bordereau_created_at_as_emitter,
        ARRAY_AGG(
            DISTINCT processing_operation
        ) filter (where processing_operation is not null)                  AS processing_operations_as_emitter
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        emitter_company_siret
),

transporter_counts AS (
    SELECT
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
    FROM
        {{ ref('transporters_bordereaux_counts_by_siret') }}
),


destination_counts AS (
    SELECT
        destination_company_siret AS siret,
        countIf(id,
            _bs_type = 'BSDD'
            AND ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                         AS num_bsdd_as_destination,
        countIf(id,
            _bs_type = 'BSDD'
            AND NOT ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                         AS num_bsdnd_as_destination,
        countIf(id,
            _bs_type = 'BSDA'
        )                         AS num_bsda_as_destination,
        countIf(id,
            _bs_type = 'BSFF'
        )                         AS num_bsff_as_destination,
        countIf(id,
            _bs_type = 'BSDASRI'
        )                         AS num_bsdasri_as_destination,
        countIf(id,
            _bs_type = 'BSVHU'
        )                         AS num_bsvhu_as_destination,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDD'
            AND ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                         AS quantity_bsdd_as_destination,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDD'
            AND NOT ({{ dangerous_waste_filter('bordereaux_enriched') }})
        )                         AS quantity_bsdnd_as_destination,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                         AS quantity_bsda_as_destination,
        SUM(accepted_quantity_packagings) FILTER (
            WHERE
            _bs_type = 'BSFF'
        )                         AS quantity_bsff_as_destination,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
        )                         AS quantity_bsdasri_as_destination,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSVHU'
        )                         AS quantity_bsvhu_as_destination,
        MAX(
            created_at
        )                         AS last_bordereau_created_at_as_destination,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSDD'
            AND ({{ dangerous_waste_filter('bordereaux_enriched') }})
            AND processing_operation is not null
        )                        AS processing_operations_as_destination_bsdd,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSDD'
            AND NOT ({{ dangerous_waste_filter('bordereaux_enriched') }})
            AND processing_operation is not null
        )                         AS processing_operations_as_destination_bsdnd,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSDA'
            AND processing_operation is not null
        )                         AS processing_operations_as_destination_bsda,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSFF'
            AND processing_operation is not null
        )                         AS processing_operations_as_destination_bsff,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSDASRI'
            AND processing_operation is not null
        )
            AS processing_operations_as_destination_bsdasri,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSVHU'
            AND processing_operation is not null
        )                         AS processing_operations_as_destination_bsvhu,
        groupArray(
            DISTINCT waste_code
        ) filter (where waste_code is not null)                         AS waste_codes_as_destination
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        destination_company_siret
),

worker_counts as (
    SELECT
        worker_company_siret AS siret,                    
        countIf(id,
            _bs_type = 'BSDA'
        )                         AS num_bsda_as_worker,
        SUM(quantity_received) FILTER (
            WHERE
            _bs_type = 'BSDA'
        )                         AS quantity_bsda_as_worker,
        MAX(
            created_at
        )                         AS last_bordereau_created_at_as_worker,
        groupArray(
            DISTINCT processing_operation
        ) FILTER (
            WHERE
            _bs_type = 'BSDA'
            AND processing_operation is not null
        )                         AS processing_operations_as_worker
    FROM
        {{ ref('bordereaux_enriched') }}
    GROUP BY
        worker_company_siret
),

full_ AS (
    SELECT
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
        COALESCE(
            emitter_counts.siret,
            transporter_counts.siret,
            destination_counts.siret,
            c.siret
        ) AS siret,
        COALESCE(
            emitter_counts.num_bsdd_as_emitter,
            0
        ) AS num_bsdd_as_emitter,
        COALESCE(
            emitter_counts.num_bsdnd_as_emitter,
            0
        ) AS num_bsdnd_as_emitter,
        COALESCE(
            emitter_counts.num_bsda_as_emitter,
            0
        ) AS num_bsda_as_emitter,
        COALESCE(
            emitter_counts.num_bsff_as_emitter,
            0
        ) AS num_bsff_as_emitter,
        COALESCE(
            emitter_counts.num_bsdasri_as_emitter,
            0
        ) AS num_bsdasri_as_emitter,
        COALESCE(
            emitter_counts.num_bsvhu_as_emitter,
            0
        ) AS num_bsvhu_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsdd_as_emitter,
            0
        ) AS quantity_bsdd_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsdnd_as_emitter,
            0
        ) AS quantity_bsdnd_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsda_as_emitter,
            0
        ) AS quantity_bsda_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsff_as_emitter,
            0
        ) AS quantity_bsff_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsdasri_as_emitter,
            0
        ) AS quantity_bsdasri_as_emitter,
        COALESCE(
            emitter_counts.quantity_bsvhu_as_emitter,
            0
        ) AS quantity_bsvhu_as_emitter,
        COALESCE(
            transporter_counts.num_bsdnd_as_transporter,
            0
        ) AS num_bsdnd_as_transporter,
        COALESCE(
            transporter_counts.num_bsdd_as_transporter,
            0
        ) AS num_bsdd_as_transporter,
        COALESCE(
            transporter_counts.num_bsda_as_transporter,
            0
        ) AS num_bsda_as_transporter,
        COALESCE(
            transporter_counts.num_bsff_as_transporter,
            0
        ) AS num_bsff_as_transporter,
        COALESCE(
            transporter_counts.num_bsdasri_as_transporter,
            0
        ) AS num_bsdasri_as_transporter,
        COALESCE(
            transporter_counts.num_bsvhu_as_transporter,
            0
        ) AS num_bsvhu_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsdd_as_transporter,
            0
        ) AS quantity_bsdd_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsdnd_as_transporter,
            0
        ) AS quantity_bsdnd_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsda_as_transporter,
            0
        ) AS quantity_bsda_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsff_as_transporter,
            0
        ) AS quantity_bsff_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsdasri_as_transporter,
            0
        ) AS quantity_bsdasri_as_transporter,
        COALESCE(
            transporter_counts.quantity_bsvhu_as_transporter,
            0
        ) AS quantity_bsvhu_as_transporter,
        COALESCE(
            destination_counts.num_bsdd_as_destination, 0
        ) AS num_bsdd_as_destination,
        COALESCE(
            destination_counts.num_bsdnd_as_destination, 0
        ) AS num_bsdnd_as_destination,
        COALESCE(
            destination_counts.num_bsda_as_destination, 0
        ) AS num_bsda_as_destination,
        COALESCE(
            destination_counts.num_bsff_as_destination, 0
        ) AS num_bsff_as_destination,
        COALESCE(
            destination_counts.num_bsdasri_as_destination, 0
        ) AS num_bsdasri_as_destination,
        COALESCE(
            destination_counts.num_bsvhu_as_destination, 0
        ) AS num_bsvhu_as_destination,
        COALESCE(
            worker_counts.num_bsda_as_worker, 0
        ) AS num_bsda_as_worker,
        COALESCE(
            destination_counts.quantity_bsdd_as_destination,
            0
        ) AS quantity_bsdd_as_destination,
        COALESCE(
            destination_counts.quantity_bsdnd_as_destination,
            0
        ) AS quantity_bsdnd_as_destination,
        COALESCE(
            destination_counts.quantity_bsda_as_destination,
            0
        ) AS quantity_bsda_as_destination,
        COALESCE(
            destination_counts.quantity_bsff_as_destination,
            0
        ) AS quantity_bsff_as_destination,
        COALESCE(
            destination_counts.quantity_bsdasri_as_destination,
            0
        ) AS quantity_bsdasri_as_destination,
        COALESCE(
            destination_counts.quantity_bsvhu_as_destination,
            0
        ) AS quantity_bsvhu_as_destination
    FROM
        emitter_counts
    FULL
    OUTER JOIN
        transporter_counts
        ON
            emitter_counts.siret = transporter_counts.siret
    FULL
    OUTER JOIN
        destination_counts
        ON
            COALESCE(emitter_counts.siret, transporter_counts.siret)
            = destination_counts.siret
    FULL
    OUTER JOIN
        worker_counts
        ON
            COALESCE(emitter_counts.siret, transporter_counts.siret, destination_counts.siret)
            = worker_counts.siret
    FULL OUTER JOIN
        {{ ref('company') }} AS c
        ON
            COALESCE(
                emitter_counts.siret,
                transporter_counts.siret,
                destination_counts.siret,
                worker_counts.siret
            )
            = c.siret
)

SELECT
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
    GREATEST(
        last_bordereau_created_at_as_emitter,
        last_bordereau_created_at_as_transporter,
        last_bordereau_created_at_as_destination,
        last_bordereau_created_at_as_worker
    )                          AS last_bordereau_created_at,
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
    + num_bsvhu_as_destination AS total_mentions_bordereaux
FROM
    full_
