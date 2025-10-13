{{
  config(
    materialized = 'ephemeral',
    )
}}

with installations as (
    select * from {{ ref('int_non_dangerous_installations') }}
),

wastes as (
    select
        b.destination_company_siret as siret,
        b.processing_operation,
        toStartOfDay(b.processed_at) as day_of_processing,
        sum(b.quantity_received)    as quantite_traitee
    from
        {{ ref('bordereaux_enriched') }} as b
    where
        b.destination_company_siret in (
            select siret
            from
                installations
        )
        and b.processed_at >= '2022-01-01'
        and (
            {{ dangerous_waste_filter('bordereaux_enriched') }}
        )
    group by
        1,3,2


)

select * from wastes