with producers as (
    SELECT 
        'producer' as role,
        emitter_company_siret as siret,
        emitter_company_name as company_name,
        emitter_departement as departement,
        count(id) as connection_count
    FROM {{ ref('bordereaux_enriched') }}
    group by 1, 2, 3, 4
),
recipients as (
    SELECT 
        'recipient' as role,
        destination_company_siret as siret,
        destination_company_name as company_name,
        destination_departement as departement,
        count(id) as connection_count
    FROM {{ ref('bordereaux_enriched') }}
    group by 1, 2, 3, 4
),

all_nodes as (
    select distinct siret, company_name, departement from producers
    union distinct
    select distinct siret, company_name, departement from recipients
)

select 
    all_nodes.siret,
    all_nodes.company_name,
    all_nodes.departement,
    arrayFilter(x -> x is not null, [
        producers.role,
        recipients.role
    ]) as roles,
    coalesce(producers.connection_count, 0) + coalesce(recipients.connection_count, 0) as connection_count
from all_nodes
left join producers on all_nodes.siret = producers.siret
left join recipients on all_nodes.siret = recipients.siret
