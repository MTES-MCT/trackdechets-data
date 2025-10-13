{{
  config(
    materialized = 'table'
    )
}}

with installations as (
    select
        i.siret,
        -- take into account the 'alineas'
        if(match(rubrique, '^2791.*'), '2791', substring(rubrique, 1, 6))
            as rubrique,
        max(raison_sociale)
            as raison_sociale,
        groupArray(distinct code_aiot)
            as codes_aiot,
        sum(quantite_totale)
            as quantite_autorisee,
        max(c.capacite_50pct)          as objectif_quantite_traitee
    from
        {{ ref('installations_rubriques_2024') }} i
    left join {{ ref('isdnd_capacites_limites_50pct') }} c on i.siret=c.siret and match(rubrique,'^2760\-2.*')        
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
        reception_date           as day_of_processing,
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
        reception_date           as day_of_processing,
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
        destination_reception_date        as day_of_processing,
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
        -- and destination_operation_code in ('D5')
        -- Actuellement le seul code final pour les BSDA. 
    group by 1, 2, 3
),

plaster_wastes as (
    select
        recipient_company_siret   as siret,
        toDate(received_at)       as day_of_processing,
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
        day_of_processing,
        code_traitement,
        quantite
    from dnd_wastes
    union all
    select
        siret,
        day_of_processing,
        code_traitement,
        quantite
    from texs_wastes
    union all
    select
        siret,
        day_of_processing,
        code_traitement,
        toFloat64(quantite) as quantite
    from bsda_wastes
    union all
    select
        siret,
        day_of_processing,
        code_traitement,
        toFloat64(quantite) as quantite
    from plaster_wastes
),

wastes_rubriques as (
    select
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique,
        sum(quantite) as quantite_traitee
    from
        wastes
    left join {{ ref('referentiel_codes_operation_rubriques') }} as mrco
        on
            wastes.code_traitement = mrco.code_operation
            and match(mrco.rubrique, '^2771.*|^2791.*|^2760\-2.*')
    group by
        wastes.siret,
        wastes.day_of_processing,
        mrco.rubrique

)

select 
    i.siret,
    i.rubrique,
    i.raison_sociale,
    i.codes_aiot,
    i.quantite_autorisee,
    i.objectif_quantite_traitee,
    wr.day_of_processing,
    wr.quantite_traitee
from installations i
left join wastes_rubriques as wr on
    installations.siret = wr.siret and installations.rubrique = wr.rubrique

