{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":4
    }
    )
}}


select
    bt.*,
    b.created_at as bordereau_created_at,
    b.emitter_company_siret,
    b.recipient_company_siret,
    b.eco_organisme_siret,
    b.waste_details_code,
    b.waste_details_is_dangerous,
    b.waste_details_pop,
    b.quantity_received,
    b.processing_operation_done
from {{ ref('bsdd_transporter') }} as bt
left join {{ ref('bsdd') }} as b on bt.form_id = b.id