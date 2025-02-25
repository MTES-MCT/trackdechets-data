{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change='append_new_columns',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
) }}

with bsff_data as (
    {{ create_bordereaux_enriched_query('bsff',False) }}
),

packagings as (
    select
        bsff_id,
        sum(acceptation_weight) as accepted_quantity_packagings
    from {{ ref('bsff_packaging') }}
    group by bsff_id
)

select
    bsff_data.*,
    packagings.accepted_quantity_packagings
from bsff_data
left join packagings on bsff_data.id = packagings.bsff_id
