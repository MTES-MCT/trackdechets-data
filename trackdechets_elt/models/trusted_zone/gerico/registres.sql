{{ config(
  pre_hook = "{{ create_indexes_for_source([
    'created',
    'created_by',
    'org_id'
    ]) }}"
) }}

with source as (
    select * from {{ source('raw_zone_gerico', 'sheets_registrydownload') }}
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
