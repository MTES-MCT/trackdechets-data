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
        toBool(count(bp.operation_date)=count(*)) AS is_processed
    FROM {{ ref('bsff') }} AS b
    FULL OUTER JOIN {{ ref('bsff_packaging') }} AS bp ON b.id = bsff_id
    WHERE
        ({{ dangerous_waste_filter('bsff') }})
        AND NOT b.is_draft
        AND NOT b.is_deleted
    GROUP BY b.id
    HAVING max(bp.operation_date) < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
),

packagings AS (
    SELECT
        bp.id AS packaging_id,
        bp.operation_code,
        bp.operation_date,
        if(bp.acceptation_weight > 60, bp.acceptation_weight / 1000,bp.acceptation_weight) AS quantity
    FROM {{ ref('bsff') }} AS b
    LEFT JOIN {{ ref('bsff_packaging') }} AS bp ON b.id = bp.bsff_id
    WHERE
        operation_date < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
        AND (
            {{ dangerous_waste_filter('bsff') }}
            OR coalesce(match(bp.acceptation_waste_code,'(?i).*\*$'), false)
        )
        AND NOT is_draft
        AND NOT is_deleted
),

bordereaux_by_week AS (
    SELECT
        toStartOfWeek(
            b.bordereaux_processed_at,1,'Europe/Paris'
        ) AS semaine,
        count(DISTINCT b.bordereau_id) FILTER (
            WHERE b.is_processed
        ) AS traitements_bordereaux
    FROM
        bordereaux AS b
    GROUP BY
        1
),

packagings_by_week AS (
    SELECT
        toStartOfWeek(
            c.operation_date,1,'Europe/Paris'
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
        1
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
