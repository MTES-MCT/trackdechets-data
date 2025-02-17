{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('raw_zone_gerico', 'registry_registrydownload') }} a
    {% if is_incremental() %}
    where a.created >= (select toString(max(created)) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("org_id") }},
        {{ adapter.quote("data_start_date") }},
        {{ adapter.quote("data_end_date") }},
        {{ adapter.quote("created") }},
        {{ adapter.quote("created_by") }}

    from source
)

select * from renamed
