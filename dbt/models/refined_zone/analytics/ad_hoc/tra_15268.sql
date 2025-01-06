{{
  config(
    materialized = 'table',
    )
}}

select
    waste_code,
    sum(quantity_received) as quantite_traitee
from
    {{ ref('bordereaux_enriched') }} as b
left join {{ ref('stock_etablissement') }} as se
    on
        b.emitter_company_siret = se.siret
where
    replace(
        se.activite_principale_etablissement, '.', '') in (
        '4399C',
        '4399D',
        '4120A',
        '4120B',
        '4211Z',
        '4212Z',
        '4213A',
        '4213B',
        '4221Z',
        '4222Z',
        '4291Z',
        '4299Z',
        '4311Z',
        '4312A',
        '4312B',
        '4313Z',
        '4321A',
        '4321B',
        '4322A',
        '4329B',
        '4391A',
        '4391B',
        '4399A',
        '4399B',
        '4329A',
        '4322B',
        '4331Z',
        '4332A',
        '4332B',
        '4332C',
        '4333Z',
        '4334Z',
        '4339Z'
    )
    and not is_draft
    and (
        waste_code ~* '.*\*$'
        or coalesce(waste_pop, false)
        or coalesce(waste_is_dangerous, false)
    )
    and processing_operation not in (
        'D9',
        'D13',
        'D14',
        'D15',
        'R12',
        'R13'
    )
group by 1
order by 2 desc nulls last
