{{ config(
    materialized = 'incremental',
    unique_key = ['id'],
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
        count(
            distinct id
        ) as num_packagings,
        count(distinct id) filter (
            where acceptation_date is not null
        ) as num_accepted_packagings,
        count(distinct id) filter (
            where operation_date is not null
        ) as num_processed_packagings,
        sum(
            acceptation_weight
        ) as accepted_quantity_packagings,
        array_agg(
            distinct operation_code
        ) as operations_codes_packagings,
        max(
            operation_date
        ) as last_operation_date_packagings
    from {{ ref('bsff_packaging') }}
    group by bsff_id
)

select
    bsff_data.*,
    packagings.num_packagings,
    packagings.num_accepted_packagings,
    packagings.num_processed_packagings,
    packagings.accepted_quantity_packagings,
    packagings.operations_codes_packagings,
    packagings.last_operation_date_packagings
from bsff_data
left join packagings on bsff_data.id = packagings.bsff_id
