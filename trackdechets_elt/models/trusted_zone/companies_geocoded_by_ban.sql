{{
  config(
    materialized = 'table',
    indexes = [
        {
            'columns': ['siret'],
            'unique':true
        }
    ]
    )
}}

with source as (
    select * from {{ source('raw_zone', 'companies_geocoded_by_ban') }}
),

renamed as (
    select
        {{ adapter.quote("siret") }},
        {{ adapter.quote("adresse") }},
        {{ adapter.quote("code_commune_insee") }},
        {{ adapter.quote("latitude") }},
        {{ adapter.quote("longitude") }},
        {{ adapter.quote("result_label") }},
        {{ adapter.quote("result_score") }},
        {{ adapter.quote("result_score_next") }},
        {{ adapter.quote("result_type") }},
        {{ adapter.quote("result_id") }},
        {{ adapter.quote("result_housenumber") }},
        {{ adapter.quote("result_name") }},
        {{ adapter.quote("result_street") }},
        {{ adapter.quote("result_postcode") }},
        {{ adapter.quote("result_city") }},
        {{ adapter.quote("result_context") }},
        {{ adapter.quote("result_citycode") }},
        {{ adapter.quote("result_oldcitycode") }},
        {{ adapter.quote("result_oldcity") }},
        {{ adapter.quote("result_district") }},
        {{ adapter.quote("result_status") }}
    from source
)

select
    siret,
    adresse,
    code_commune_insee,
    latitude::float,
    longitude::float,
    result_label,
    result_score::float,
    result_score_next::float,
    result_type,
    result_id,
    result_housenumber,
    result_name,
    result_street,
    result_postcode,
    result_city,
    result_context,
    result_citycode,
    result_oldcitycode,
    result_oldcity,
    result_district,
    result_status
from renamed
