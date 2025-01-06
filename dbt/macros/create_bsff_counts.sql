{% macro create_bsff_counts(
        date_column_name,
        count_name,
        quantity_name
    ) -%}

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
            b.id as bordereau_id,
            bp.id as packaging_id,
            bp.operation_code as "operation_code",
            case
                when bp.acceptation_weight > 60 then bp.acceptation_weight / 1000
                else bp.acceptation_weight
            END as "quantity",
            {{ date_column_name }}
        FROM {{ ref('bsff') }} b
        LEFT JOIN {{ ref('bsff_packaging') }} bp ON b.id = bp.bsff_id
        WHERE
            {{ date_column_name }} < DATE_TRUNC('week',now())
            AND (b.waste_code ~* '.*\*$' or coalesce(bp.acceptation_waste_code ~* '.*\*$',false))
            and not is_draft
            and not is_deleted
    )
    SELECT
        DATE_TRUNC(
            'week',
            {{ date_column_name }}
        )::timestamp AS "semaine",
        count(distinct bordereau_id) as {{ count_name }}_bordereaux,
        count(distinct packaging_id) as {{ count_name }}_contenants,
        sum("quantity") as {{ quantity_name }}
        {% if count_name=="traitements"  %}
        ,COUNT(distinct bordereau_id) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS {{ count_name }}_bordereaux_operations_non_finales,
        COUNT(distinct packaging_id) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS {{ count_name }}_contenants_operations_non_finales, 
        sum(quantity) FILTER (WHERE operation_code in {{ non_final_operation_codes }}) AS {{ quantity_name }}_operations_non_finales,   
        COUNT(distinct bordereau_id) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS {{ count_name }}_bordereaux_operations_finales,
        COUNT(distinct packaging_id) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS {{ count_name }}_contenants_operations_finales,
        sum(quantity) FILTER (WHERE operation_code not in {{ non_final_operation_codes }}) AS {{ quantity_name }}_operations_finales
        {% endif %}
    FROM
        bordereaux
    GROUP BY
        DATE_TRUNC(
            'week',
            {{ date_column_name }}
        )
    ORDER BY
        DATE_TRUNC(
            'week',
            {{ date_column_name }}
        )
{%- endmacro %}
