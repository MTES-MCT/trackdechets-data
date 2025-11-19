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
            if(bp.acceptation_weight > 60,bp.acceptation_weight / 1000,bp.acceptation_weight) as "quantity",
            {{ date_column_name }}
        FROM {{ ref('bsff') }} b
        LEFT JOIN {{ ref('bsff_packaging') }} bp ON b.id = bp.bsff_id
        WHERE
            {{ date_column_name }} < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
            AND (match(b.waste_code,'(?i).*\*$') or coalesce(match(bp.acceptation_waste_code,'(?i).*\*$'),false))
            and not is_draft
            and not is_deleted
    )
    SELECT
        toStartOfWeek({{ date_column_name }},1,'Europe/Paris') AS "semaine",
        count(distinct bordereau_id) as {{ count_name }}_bordereaux,
        count(distinct packaging_id) as {{ count_name }}_contenants,
        sum("quantity") as {{ quantity_name }}
    {% if count_name=="traitements" %}
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
        1
    ORDER BY
        1
{%- endmacro %}
