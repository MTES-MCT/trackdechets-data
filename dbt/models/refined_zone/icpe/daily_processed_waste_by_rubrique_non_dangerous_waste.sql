{{
  config(
    materialized = 'table'
    )
}}

with installations as (
    select
        siret,
        -- take into account the 'alineas'
        if(match(rubrique, '^2791.*'), '2791', substring(rubrique, 1, 6))
            as rubrique,
        max(raison_sociale)
            as raison_sociale,
        groupArray(distinct code_aiot)
            as codes_aiot,
        sum(quantite_totale)
            as quantite_autorisee
    from
        {{ ref('installations_rubriques_2024') }}
    where
        siret is not null
        and match(rubrique, '^2771.*|^2791.*|^2760\-2.*')
        and etat_technique_rubrique = 'Exploité'
        and etat_administratif_rubrique = 'En vigueur'
        and libelle_etat_site = 'Avec titre'
    group by
        1,
        2
),

dnd_wastes as (
    select
        report_for_company_siret as siret,
        reception_date           as date_reception,
        operation_code           as code_traitement,
        sum(weight_value)        as quantite
    from {{ ref('registry_incoming_waste') }}
    where
        reception_date >= '2022-01-01'
        and report_for_company_siret in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

texs_wastes as (
    select
        report_for_company_siret as siret,
        reception_date           as date_reception,
        operation_code           as code_traitement,
        sum(weight_value)        as quantite
    from {{ ref('registry_incoming_texs') }}
    where
        reception_date >= '2022-01-01'
        and report_for_company_siret in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

bsda_wastes as (
    select
        destination_company_siret         as siret,
        destination_reception_date        as date_reception,
        destination_operation_code        as code_traitement,
        sum(destination_reception_weight) as quantite
    from {{ ref('bsda') }}
    where
        destination_reception_date >= '2022-01-01'
        and destination_company_siret in (
            select siret
            from
                installations
        )
    group by 1, 2, 3
),

plater_wastes as (
    select
        recipient_company_siret   as siret,
        toDate(received_at)       as date_reception,
        processing_operation_done as code_traitement,
        sum(quantity_received)    as quantite
    from {{ ref('bsdd') }}
    where
        received_at >= '2022-01-01'
        and recipient_company_siret in (
            select siret
            from
                installations
        )
        and waste_details_code = '17 08 01*'
    group by 1, 2, 3

),

wastes as (
    select
        siret,
        date_reception,
        code_traitement,
        quantite
    from dnd_wastes
    union all
    select
        siret,
        date_reception,
        code_traitement,
        quantite
    from texs_wastes
    union all
    select
        siret,
        date_reception,
        code_traitement,
        toFloat64(quantite) as quantite
    from bsda_wastes
    union all
    select
        siret,
        date_reception,
        code_traitement,
        toFloat64(quantite) as quantite
    from plater_wastes
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique,
        sum(quantite) as quantite
    from
        wastes
    inner join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.code_traitement = mrco.code_operation
            and match(mrco.rubrique, '^2771.*|^2791.*|^2760\-2.*')
    group by
        wastes.siret,
        wastes.date_reception,
        mrco.rubrique

)

select *
from wastes_rubriques
