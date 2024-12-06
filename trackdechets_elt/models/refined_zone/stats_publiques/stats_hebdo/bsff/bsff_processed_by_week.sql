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

WITH bordereaux as (
    SELECT
        b.id as bordereau_id,
        max(bp.operation_date) as "bordereaux_processed_at",
        bool_and(bp.operation_date is not null) as is_processed
    FROM {{ ref('bsff') }} b
    full outer join {{ ref('bsff_packaging') }} bp on b.id=bsff_id
    WHERE
        (b.waste_code ~* '.*\*$')
        and not b.is_draft
        and not b.is_deleted
    group by b.id
    having max(bp.operation_date) < DATE_TRUNC('week',now())
),

packagings AS (
    SELECT
        bp.id as packaging_id,
        bp.operation_code as "operation_code",
        case
            when bp.acceptation_weight > 60 then bp.acceptation_weight / 1000
            else bp.acceptation_weight
        END as "quantity",
        bp.operation_date
    FROM {{ ref('bsff') }} b
    LEFT JOIN {{ ref('bsff_packaging') }} bp ON b.id = bp.bsff_id
    WHERE
        operation_date < DATE_TRUNC('week',now())
        AND (b.waste_code ~* '.*\*$' or coalesce(bp.acceptation_waste_code ~* '.*\*$',false))
        and not is_draft
        and not is_deleted
),

bordereaux_by_week as (
    select
        DATE_TRUNC(
            'week',
            b.bordereaux_processed_at
        ) as "semaine",
        count(distinct bordereau_id) filter (
        where is_processed) as traitements_bordereaux
    from
        bordereaux b
    group by
        DATE_TRUNC(
            'week',
            b.bordereaux_processed_at
        )
),

packagings_by_week as (
    select
        DATE_TRUNC(
            'week',
        c.operation_date
        ) as "semaine",
        count(distinct packaging_id) as traitements_contenants,
        sum("quantity") as quantite_traitee,
        COUNT(distinct packaging_id) filter (
        where operation_code in {{non_final_operation_codes}}) as traitements_contenants_operations_non_finales, 
        sum(quantity) filter (
        where operation_code in {{non_final_operation_codes}}) as quantite_traitee_operations_non_finales,   
        COUNT(distinct packaging_id) filter (
        where operation_code not in {{non_final_operation_codes}}) as traitements_contenants_operations_finales,
        sum(quantity) filter (
        where operation_code not in {{non_final_operation_codes}}) as quantite_traitee_operations_finales
    from
        packagings c
    group by
        DATE_TRUNC(
            'week',
            c.operation_date
        )
)

select
    coalesce(p.semaine,
    b.semaine) as semaine,
    traitements_bordereaux,
    traitements_contenants,
    quantite_traitee,
    traitements_contenants_operations_non_finales,
    quantite_traitee_operations_non_finales,
    traitements_contenants_operations_finales,
    quantite_traitee_operations_finales
from
    packagings_by_week p
left outer join bordereaux_by_week b on
    p.semaine = b.semaine
order by
    semaine desc
