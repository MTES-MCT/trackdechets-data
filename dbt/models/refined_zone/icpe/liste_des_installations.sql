{{
  config(
    materialized = 'table',
    indexes = [
        {
            'columns':['siret'],
            'unique': true
        }
    ]
    )
}}

with td_installations as (
    select
        destination_company_siret                as siret,
        max(destination_company_name)            as company_name,
        max(destination_company_address)         as company_address,
        array_agg(distinct processing_operation) as codes_operation,
        array_agg(distinct _bs_type)             as types_bordereaux
    from
        {{ ref('bordereaux_enriched') }}
    group by 1
),

gun_installations as (
    select
        siret,
        max(raison_sociale)                            as raison_sociale,
        array_agg(distinct code_aiot)                  as codes_aiot,
        array_agg(distinct rubrique order by rubrique) as rubriques
    from
        {{ ref('installations_rubriques_2024') }}
    where
        (libelle_etat_site = 'Avec titre') -- noqa: LXR
        and (etat_administratif_rubrique in ('En vigueur', 'A l''arrêt'))
        and (etat_technique_rubrique = 'Exploité')
        and (raison_sociale !~* 'illégal|illicite')
    group by 1
),

joined_data as (
    select
        ti.company_name,
        gi.raison_sociale,
        ti.company_address,
        ti.codes_operation,
        ti.types_bordereaux,
        gi.codes_aiot,
        gi.rubriques,
        coalesce(ti.siret, gi.siret) as siret,
        ti.siret is not null         as present_td,
        gi.siret is not null         as present_gun
    from
        td_installations as ti
    full outer join gun_installations as gi on ti.siret = gi.siret
    where
        char_length(coalesce(ti.siret, gi.siret)) = 14
        and coalesce(ti.siret, gi.siret) is not null
)

select
    jd.siret,
    cgc.code_commune,
    cgc.code_departement,
    jd.present_td,
    jd.present_gun,
    jd.codes_operation,
    jd.types_bordereaux,
    jd.codes_aiot,
    jd.rubriques,
    coalesce(
        c.name,
        jd.company_name,
        jd.raison_sociale,
        se.enseigne_1_etablissement,
        se.enseigne_2_etablissement,
        se.enseigne_3_etablissement,
        se.denomination_usuelle_etablissement
    ) as nom_etablissement,
    coalesce(
        c.address,
        jd.company_address,
        coalesce(se.complement_adresse_etablissement || ' ', '')
        || coalesce(se.numero_voie_etablissement || ' ', '')
        || coalesce(se.indice_repetition_etablissement || ' ', '')
        || coalesce(se.type_voie_etablissement || ' ', '')
        || coalesce(se.libelle_voie_etablissement || ' ', '')
        || coalesce(se.code_postal_etablissement || ' ', '')
        || coalesce(se.libelle_commune_etablissement || ' ', '')
        || coalesce(se.libelle_commune_etranger_etablissement || ' ', '')
        || coalesce(se.distribution_speciale_etablissement, '')
    ) as adresse
from joined_data as jd
left join {{ ref('company') }} as c on jd.siret = c.siret
left join {{ ref('stock_etablissement') }} as se on jd.siret = se.siret
left join
    {{ ref("code_geo_communes") }} as cgc
    on
        se.code_commune_etablissement = cgc.code_commune
        and cgc.type_commune != 'COMD'
