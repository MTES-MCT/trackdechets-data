{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select *
    from {{ source('raw_zone_gerico', 'sheets_computedinspectiondata') }} a
    {% if is_incremental() %}
    where a.created >= (select toString(max(created)) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("org_id") }},
        {{ adapter.quote("created") }},
        {{ adapter.quote("state") }},
        {{ adapter.quote("created_by") }},
        {{ adapter.quote("data_end_date") }},
        {{ adapter.quote("data_start_date") }},
        {{ adapter.quote("creation_mode") }},
        {{ adapter.quote("pdf_rendering_end") }},
        {{ adapter.quote("pdf_rendering_start") }},
        {{ adapter.quote("processing_end") }},
        {{ adapter.quote("processing_start") }}
    from source
)

select * from renamed
