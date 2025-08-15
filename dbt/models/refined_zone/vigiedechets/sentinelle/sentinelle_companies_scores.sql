{{
  config(
    materialized = 'table',
    )
}}


with scores as (
    select
        fgd.siret,
        max(fgd.first_activity_datetime) as first_activity_datetime,
        max(fgd.total_events_count)      as total_events_count,
        max(
            fgd.ape_code
        )                                as ape_code,
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
        )                                as scores
    from
        {{ ref('sentinelle_companies_features') }} as fgd
    group by
        1
)

select *
from
    scores
