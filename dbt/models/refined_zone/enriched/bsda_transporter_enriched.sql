{{
  config(
    materialized = 'table',
    )
}}


select
    bt.*,
    b.created_at as bordereau_created_at,
    b.emitter_company_siret,
    b.destination_company_siret,
    b.worker_company_siret,
    b.eco_organisme_siret,
    b.waste_code,
    b.destination_reception_weight,
    b.destination_operation_code
from {{ ref('bsda_transporter') }} as bt
left join {{ ref('bsda') }} as b on bt.bsda_id = b.id
