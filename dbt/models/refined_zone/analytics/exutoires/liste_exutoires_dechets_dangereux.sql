{{
  config(
    materialized = 'table',
    indexes = [{"columns":["siret"],"unique":True}]
    )
}}

select
    destination_company_siret as siret,
    count(*) filter (
        where
        _bs_type = 'BSDD'
    ) > 0                     as traiteur_bsdd,
    count(*) filter (
        where
        _bs_type = 'BSDA'
    ) > 0                     as traiteur_bsda,
    count(*) filter (
        where
        _bs_type = 'BSFF'
    ) > 0                     as traiteur_bsff,
    count(*) filter (
        where
        _bs_type = 'BSDASRI'
    ) > 0                     as traiteur_bsdasri,
    count(*) filter (
        where
        _bs_type = 'BSVHU'
    ) > 0                     as traiteur_bsvhu
from
    {{ ref('bordereaux_enriched') }}
where
    (
        waste_code ~* '.*\*$'
        or waste_pop
        or waste_is_dangerous
    )
    and (
        processing_operation
        not in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
    )
    and processed_at is not null
group by
    destination_company_siret
