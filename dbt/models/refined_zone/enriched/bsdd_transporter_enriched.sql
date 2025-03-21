{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
    )
}}


select
    bt.*,
    b.created_at as bordereau_created_at,
    b.updated_at as bordereau_updated_at,
    b.status,
    b.emitter_company_siret,
    b.emitter_type,
    b.recipient_company_siret,
    b.eco_organisme_siret,
    b.waste_details_code,
    b.waste_details_is_dangerous,
    b.waste_details_pop,
    b.waste_details_quantity,
    b.quantity_received,
    b.quantity_refused,
    b.processing_operation_done
from {{ ref('bsdd_transporter') }} as bt
left join {{ ref('bsdd') }} as b on bt.form_id = b.id