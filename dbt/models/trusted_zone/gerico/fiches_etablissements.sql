{{ config(
  pre_hook = "{{ create_indexes_for_source([
    'created',
    'created_by',
    'org_id',
    'creation_mode'
    ]) }}"
) }}

with source as (
    select *
    from {{ source('raw_zone_gerico', 'sheets_computedinspectiondata') }}
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
