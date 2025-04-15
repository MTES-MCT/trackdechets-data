{{ config(
    materialized = 'incremental',
    unique_key = ['id'],
    on_schema_change='append_new_columns',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":8
    }
) }}

{{ create_bordereaux_enriched_query('bsda',False) }}
