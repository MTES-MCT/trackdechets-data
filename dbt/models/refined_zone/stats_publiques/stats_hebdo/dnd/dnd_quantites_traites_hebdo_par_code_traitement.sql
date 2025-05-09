{{
  config(
    materialized = 'table',
    )
}}

select
    toStartOfWeek(
        reception_date,1
    )                                  as semaine,
    operation_code                    as code_operation,
    multiIf(
            operation_code like 'R%','Déchet valorisé',
            operation_code like 'D%','Déchet éliminé',
            'Autre'
    )                     as type_operation,
    sum(
        if(
            weight_value > 60,
            weight_value / 1000,
            weight_value
        )
        
    ) as quantite_traitee,
    sum(
        if(
             volume > 60,
            volume / 1000,
            volume
        )
    ) as volume_traite
from {{ ref('registry_incoming_waste') }}
group by 1, 2
order by 1 desc
