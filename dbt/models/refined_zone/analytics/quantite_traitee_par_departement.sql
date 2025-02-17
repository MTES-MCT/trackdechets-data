{{
  config(
    materialized = 'table',
    )
}}

select
    be.destination_departement as departement,
    be.processing_operation    as operation_de_traitement,
    max(cgd.nom_en_clair)      as nom_departement,
    max(be.destination_region) as code_region,
    sum(be.quantity_received)  as quantite_traitee
from {{ ref('bordereaux_enriched') }} as be
left join
    {{ ref('code_geo_departements') }} as cgd
    on be.destination_departement = cgd.code_departement
where
    be.processing_operation not in (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
    and date_part('year', be.processed_at) = 2023
group by be.destination_departement, be.processing_operation
