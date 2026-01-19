with producer_destination_edges as (
    SELECT 
            'producteur_destination' as relation_type,
            emitter_company_siret || '_' || destination_company_siret as id,
            emitter_company_siret as source,
            destination_company_siret as target,
            count(id) as weight,
            array_agg(distinct "_bs_type") as bsd_types
    FROM {{ ref('bordereaux_enriched') }}
    group by 1, 2, 3, 4
),

all_edges as (
    select 
        relation_type,
        id,
        source,
        target,
        weight,
        bsd_types
    from producer_destination_edges
)

select * from all_edges