{{
  config(
    materialized = 'table',
    indexes = [{"columns":["siret"],"unique": True}]
    )
}}

with dataset as (
    select
        id,
        emitter_company_siret,
        transporter_company_siret,
        destination_company_siret,
        worker_company_siret,
        processing_operation
    from
        {{ ref('bordereaux_enriched') }}
    where
        created_at >= '2022-01-01'
        and not is_draft
),

producteurs as (
    select
        emitter_company_siret as siret,
        'producteur'          as type_etablissement,
        count(*)              as num_bordereaux
    from
        dataset
    where
        emitter_company_siret is not null
    group by
        emitter_company_siret
),

ttr as (
    select
        destination_company_siret as siret,
        'TTR'                     as type_etablissement,
        count(*)                  as num_bordereaux
    from
        dataset
    where
        processing_operation in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and destination_company_siret is not null
    group by
        destination_company_siret
),

exutoires as (
    select
        destination_company_siret as siret,
        'exutoire'                as type_etablissement,
        count(*)                  as num_bordereaux
    from
        dataset
    where
        processing_operation not in (
            'D9',
            'D13',
            'D14',
            'D15',
            'R12',
            'R13'
        )
        and destination_company_siret is not null
    group by
        destination_company_siret
),

transporteurs as (
    select
        transporter_company_siret as siret,
        'transporteur'            as type_etablissement,
        count(*)                  as num_bordereaux
    from
        dataset
    where
        transporter_company_siret is not null
    group by
        transporter_company_siret

),

travaux as (
    select
        worker_company_siret    as siret,
        'entreprise de travaux' as type_etablissement,
        count(*)                as num_bordereaux
    from
        dataset
    where
        worker_company_siret is not null
    group by
        worker_company_siret
),

merged as (
    select *
    from
        producteurs
    union all
    select *
    from
        ttr
    union all
    select *
    from
        exutoires
    union all
    select *
    from
        transporteurs
    union all
    select *
    from
        travaux
)

select
    siret,
    sum(
        num_bordereaux
    ) as num_mentions_bordereaux,
    max(
        type_etablissement order by num_bordereaux
    ) as type_etablissement
from
    merged
group by
    siret
