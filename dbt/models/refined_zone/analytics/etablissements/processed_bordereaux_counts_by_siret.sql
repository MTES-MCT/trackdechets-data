{{ config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'],
    'unique': True },],
) }}

with bordereaux_data as (
    select
        be._bs_type,
        be.id,
        be.destination_company_siret,
        be.eco_organisme_siret,
        be.emitter_company_siret,
        be.transporter_company_siret,
        be.worker_company_siret,
        be.quantity_received
    from
        {{ ref('bordereaux_enriched') }} as be
    where
        be.processed_at is not null
        and not be.is_draft
        and (
            be.waste_code ~* '.*\*$'
            or coalesce(
                be.waste_pop,
                false
            )
            or coalesce(
                be.waste_is_dangerous,
                false
            )
        )

),

emitter_counts as (
    select
        emitter_company_siret as siret,
        count(id) filter (
            where
            _bs_type = 'BSDD'
        )                     as num_bsdd_as_emitter,
        count(id) filter (
            where
            _bs_type = 'BSDA'
        )                     as num_bsda_as_emitter,
        count(id) filter (
            where
            _bs_type = 'BSFF'
        )                     as num_bsff_as_emitter,
        count(id) filter (
            where
            _bs_type = 'BSDASRI'
        )                     as num_bsdasri_as_emitter,
        count(id) filter (
            where
            _bs_type = 'BSVHU'
        )                     as num_bsvhu_as_emitter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDD'
        )                     as quantity_bsdd_as_emitter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDA'
        )                     as quantity_bsda_as_emitter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSFF'
        )                     as quantity_bsff_as_emitter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDASRI'
        )                     as quantity_bsdasri_as_emitter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSVHU'
        )                     as quantity_bsvhu_as_emitter
    from
        bordereaux_data
    group by
        emitter_company_siret
),

transporter_counts as (
    select
        transporter_company_siret as siret,
        count(id) filter (
            where
            _bs_type = 'BSDD'
        )                         as num_bsdd_as_transporter,
        count(id) filter (
            where
            _bs_type = 'BSDA'
        )                         as num_bsda_as_transporter,
        count(id) filter (
            where
            _bs_type = 'BSFF'
        )                         as num_bsff_as_transporter,
        count(id) filter (
            where
            _bs_type = 'BSDASRI'
        )                         as num_bsdasri_as_transporter,
        count(id) filter (
            where
            _bs_type = 'BSVHU'
        )                         as num_bsvhu_as_transporter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDD'
        )                         as quantity_bsdd_as_transporter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDA'
        )                         as quantity_bsda_as_transporter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSFF'
        )                         as quantity_bsff_as_transporter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDASRI'
        )                         as quantity_bsdasri_as_transporter,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSVHU'
        )                         as quantity_bsvhu_as_transporter
    from
        bordereaux_data
    group by
        transporter_company_siret
),

destination_counts as (
    select
        destination_company_siret as siret,
        count(id) filter (
            where
            _bs_type = 'BSDD'
        )                         as num_bsdd_as_destination,
        count(id) filter (
            where
            _bs_type = 'BSDA'
        )                         as num_bsda_as_destination,
        count(id) filter (
            where
            _bs_type = 'BSFF'
        )                         as num_bsff_as_destination,
        count(id) filter (
            where
            _bs_type = 'BSDASRI'
        )                         as num_bsdasri_as_destination,
        count(id) filter (
            where
            _bs_type = 'BSVHU'
        )                         as num_bsvhu_as_destination,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDD'
        )                         as quantity_bsdd_as_destination,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDA'
        )                         as quantity_bsda_as_destination,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSFF'
        )                         as quantity_bsff_as_destination,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSDASRI'
        )                         as quantity_bsdasri_as_destination,
        sum(quantity_received) filter (
            where
            _bs_type = 'BSVHU'
        )                         as quantity_bsvhu_as_destination
    from
        bordereaux_data
    group by
        destination_company_siret
),

full_ as (
    select
        coalesce(
            emitter_counts.siret,
            transporter_counts.siret,
            destination_counts.siret,
            c.siret
        ) as siret,
        coalesce(
            emitter_counts.num_bsdd_as_emitter,
            0
        ) as num_bsdd_as_emitter,
        coalesce(
            emitter_counts.num_bsda_as_emitter,
            0
        ) as num_bsda_as_emitter,
        coalesce(
            emitter_counts.num_bsff_as_emitter,
            0
        ) as num_bsff_as_emitter,
        coalesce(
            emitter_counts.num_bsdasri_as_emitter,
            0
        ) as num_bsdasri_as_emitter,
        coalesce(
            emitter_counts.num_bsvhu_as_emitter,
            0
        ) as num_bsvhu_as_emitter,
        coalesce(
            emitter_counts.quantity_bsdd_as_emitter,
            0
        ) as quantity_bsdd_as_emitter,
        coalesce(
            emitter_counts.quantity_bsda_as_emitter,
            0
        ) as quantity_bsda_as_emitter,
        coalesce(
            emitter_counts.quantity_bsff_as_emitter,
            0
        ) as quantity_bsff_as_emitter,
        coalesce(
            emitter_counts.quantity_bsdasri_as_emitter,
            0
        ) as quantity_bsdasri_as_emitter,
        coalesce(
            emitter_counts.quantity_bsvhu_as_emitter,
            0
        ) as quantity_bsvhu_as_emitter,
        coalesce(
            transporter_counts.num_bsdd_as_transporter,
            0
        ) as num_bsdd_as_transporter,
        coalesce(
            transporter_counts.num_bsda_as_transporter,
            0
        ) as num_bsda_as_transporter,
        coalesce(
            transporter_counts.num_bsff_as_transporter,
            0
        ) as num_bsff_as_transporter,
        coalesce(
            transporter_counts.num_bsdasri_as_transporter,
            0
        ) as num_bsdasri_as_transporter,
        coalesce(
            transporter_counts.num_bsvhu_as_transporter,
            0
        ) as num_bsvhu_as_transporter,
        coalesce(
            transporter_counts.quantity_bsdd_as_transporter,
            0
        ) as quantity_bsdd_as_transporter,
        coalesce(
            transporter_counts.quantity_bsda_as_transporter,
            0
        ) as quantity_bsda_as_transporter,
        coalesce(
            transporter_counts.quantity_bsff_as_transporter,
            0
        ) as quantity_bsff_as_transporter,
        coalesce(
            transporter_counts.quantity_bsdasri_as_transporter,
            0
        ) as quantity_bsdasri_as_transporter,
        coalesce(
            transporter_counts.quantity_bsvhu_as_transporter,
            0
        ) as quantity_bsvhu_as_transporter,
        coalesce(
            destination_counts.num_bsdd_as_destination, 0
        ) as num_bsdd_as_destination,
        coalesce(
            destination_counts.num_bsda_as_destination, 0
        ) as num_bsda_as_destination,
        coalesce(
            destination_counts.num_bsff_as_destination, 0
        ) as num_bsff_as_destination,
        coalesce(
            destination_counts.num_bsdasri_as_destination, 0
        ) as num_bsdasri_as_destination,
        coalesce(
            destination_counts.num_bsvhu_as_destination, 0
        ) as num_bsvhu_as_destination,
        coalesce(
            destination_counts.quantity_bsdd_as_destination,
            0
        ) as quantity_bsdd_as_destination,
        coalesce(
            destination_counts.quantity_bsda_as_destination,
            0
        ) as quantity_bsda_as_destination,
        coalesce(
            destination_counts.quantity_bsff_as_destination,
            0
        ) as quantity_bsff_as_destination,
        coalesce(
            destination_counts.quantity_bsdasri_as_destination,
            0
        ) as quantity_bsdasri_as_destination,
        coalesce(
            destination_counts.quantity_bsvhu_as_destination,
            0
        ) as quantity_bsvhu_as_destination
    from
        emitter_counts
    full
    outer join
        transporter_counts
        on
            emitter_counts.siret = transporter_counts.siret
    full
    outer join
        destination_counts
        on
            coalesce(emitter_counts.siret, transporter_counts.siret)
            = destination_counts.siret
    full outer join
        {{ ref('company') }} as c
        on
            coalesce(
                emitter_counts.siret,
                transporter_counts.siret,
                destination_counts.siret
            )
            = c.siret
)

select
    *,
    num_bsdd_as_emitter
    + num_bsda_as_emitter
    + num_bsff_as_emitter
    + num_bsdasri_as_emitter
    + num_bsvhu_as_emitter
    + num_bsdd_as_transporter
    + num_bsda_as_transporter
    + num_bsff_as_transporter
    + num_bsdasri_as_transporter
    + num_bsvhu_as_transporter
    + num_bsdd_as_destination
    + num_bsda_as_destination
    + num_bsff_as_destination
    + num_bsdasri_as_destination
    + num_bsvhu_as_destination as total_mentions_bordereaux
from
    full_
