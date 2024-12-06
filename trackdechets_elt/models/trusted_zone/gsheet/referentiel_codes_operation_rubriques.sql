with source as (
    select *
    from
        {{ source('raw_zone_gsheet', 'referentiel_codes_operation_rubriques') }}
),

renamed as (
    select
        {{ adapter.quote("code_operation") }},
        {{ adapter.quote("rubrique") }},
        {{ adapter.quote("criteres") }},
        {{ adapter.quote("unite") }}

    from source
)

select * from renamed
