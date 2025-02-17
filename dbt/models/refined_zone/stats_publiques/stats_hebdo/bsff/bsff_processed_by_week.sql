{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{% set non_final_operation_codes %}
    (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
{% endset %}

WITH bordereaux AS (
    SELECT
        b.id                                    AS bordereau_id,
        max(bp.operation_date)                  AS bordereaux_processed_at,
        bool_and(bp.operation_date IS NOT null) AS is_processed
    FROM {{ ref('bsff') }} AS b
    FULL OUTER JOIN {{ ref('bsff_packaging') }} AS bp ON b.id = bsff_id
    WHERE
        (b.waste_code ~* '.*\*$')
        AND NOT b.is_draft
        AND NOT b.is_deleted
    GROUP BY b.id
    HAVING max(bp.operation_date) < date_trunc('week', now())
),

packagings AS (
    SELECT
        bp.id AS packaging_id,
        bp.operation_code,
        bp.operation_date,
        CASE
            WHEN bp.acceptation_weight > 60 THEN bp.acceptation_weight / 1000
            ELSE bp.acceptation_weight
        END   AS quantity
    FROM {{ ref('bsff') }} AS b
    LEFT JOIN {{ ref('bsff_packaging') }} AS bp ON b.id = bp.bsff_id
    WHERE
        operation_date < date_trunc('week', now())
        AND (
            b.waste_code ~* '.*\*$'
            OR coalesce(bp.acceptation_waste_code ~* '.*\*$', false)
        )
        AND NOT is_draft
        AND NOT is_deleted
),

bordereaux_by_week AS (
    SELECT
        date_trunc(
            'week',
            b.bordereaux_processed_at
        ) AS semaine,
        count(DISTINCT b.bordereau_id) FILTER (
            WHERE b.is_processed
        ) AS traitements_bordereaux
    FROM
        bordereaux AS b
    GROUP BY
        date_trunc(
            'week',
            b.bordereaux_processed_at
        )
),

packagings_by_week AS (
    SELECT
        date_trunc(
            'week',
            c.operation_date
        )                              AS semaine,
        count(DISTINCT c.packaging_id) AS traitements_contenants,
        sum(c.quantity)                AS quantite_traitee,
        count(DISTINCT c.packaging_id) FILTER (
            WHERE c.operation_code IN {{ non_final_operation_codes }}        )
            AS traitements_contenants_operations_non_finales,
        sum(c.quantity) FILTER (
            WHERE c.operation_code IN {{ non_final_operation_codes }}        )
            AS quantite_traitee_operations_non_finales,
        count(DISTINCT c.packaging_id) FILTER (
            WHERE c.operation_code NOT IN {{ non_final_operation_codes }}        )
            AS traitements_contenants_operations_finales,
        sum(c.quantity) FILTER (
            WHERE c.operation_code NOT IN {{ non_final_operation_codes }}        )
            AS quantite_traitee_operations_finales
    FROM
        packagings AS c
    GROUP BY
        date_trunc(
            'week',
            c.operation_date
        )
)

SELECT
    traitements_bordereaux,
    traitements_contenants,
    quantite_traitee,
    traitements_contenants_operations_non_finales,
    quantite_traitee_operations_non_finales,
    traitements_contenants_operations_finales,
    quantite_traitee_operations_finales,
    coalesce(
        p.semaine,
        b.semaine
    ) AS semaine
FROM
    packagings_by_week AS p
LEFT OUTER JOIN bordereaux_by_week AS b
    ON
        p.semaine = b.semaine
ORDER BY
    semaine DESC
