{{
  config(
    materialized = 'table',
    order_by = '(company_ape_code,company_siret)',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":128
    }
    )
}}


with scores as (
    select
        fgd.company_siret,
        max(
            fgd.company_ape_code
        )                                as ape_code,
        max(fgd.first_activity_datetime) as first_activity_datetime,
        max(fgd.total_events_count)      as total_events_count,
        max(fgd.company_name)            as company_name,
        max(company_address)             as company_address,
        max(company_code_postal)         as company_code_postal,
        max(company_libelle_commune)     as company_libelle_commune,
        max(company_code_commune)        as company_code_commune,
        max(company_code_departement)    as company_code_departement,
        max(company_code_region)         as company_code_region,
        groupArray(
            '{\'waste_code\': \''
            || fgd.waste_code
            || '\', '
            || '\'quantity_share\':'
            || toDecimalString(round(fgd.waste_quantity_share, 6), 8)
            || ', \'quantity_share_ref\':'
            || toDecimalString(round(fgd.waste_quantity_share_ref, 6), 8)
            || '}')
            as score_details, -- constructs an array of json objects with score details for each waste code
        sum(
            abs(fgd.waste_quantity_share - fgd.waste_quantity_share_ref)
            * fgd.waste_quantity_share_ref
        )                                as score
    from
        {{ ref('sentinelle_companies_features') }} as fgd
    group by
        1
)

select
    s.company_siret,
    s.company_name,
    s.company_address,
    s.company_code_postal,
    s.company_libelle_commune,
    s.company_code_commune,
    s.company_code_departement,
    s.company_code_region,
    s.ape_code
        as company_ape_code,
    s.first_activity_datetime,
    s.total_events_count,
    s.score_details,
    s.score
from
    scores as s
