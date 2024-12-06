{{
  config(
    materialized = 'table',
    indexes = [ {'columns': ['siret'], 'unique': True }]
    )
}}

with hardcoded_eo as (
    select
        *,
        1 as "dans_table_eo"
    from
        {{ ref('eco_organisme') }}
),

eo_agrees as (
    select distinct on
    (eoa.siret)
        eoa.siret,
        eoa.nom_eco_organisme
    from
        {{ ref('eco_organismes_agrees_2022') }} as eoa
),

companies_eo as (
    select
        id,
        siret,
        name,
        case
            when
                array_length(c.eco_organisme_agreements, 1) != 0 then 1
            else 0
        end as "a_entré_un_agrément_eo",
        case
            when
                'ECO_ORGANISME' = any(company_types) then 1
            else 0
        end as "a_choisi_profil_eo"
    from
        {{ ref('company') }} as c
    where
        array_length(c.eco_organisme_agreements, 1) != 0
        or 'ECO_ORGANISME' = any(company_types)
),

bsdd_eo as (
    select distinct on
    (eco_organisme_siret)
        eco_organisme_siret as siret,
        eco_organisme_name  as name,
        1                   as "visé_dans_bsdd"
    from
        {{ ref('bsdd') }} as b
    where
        b.eco_organisme_siret is not null
),

bsda_eo as (
    select distinct on
    (eco_organisme_siret)
        eco_organisme_siret as siret,
        eco_organisme_name  as name,
        1                   as "visé_dans_bsda"
    from
        {{ ref('bsda') }} as b2
    where
        b2.eco_organisme_siret is not null
),

bsdasri_eo as (
    select distinct on
    (eco_organisme_siret)
        eco_organisme_siret as siret,
        eco_organisme_name  as name,
        1                   as "visé_dans_bsdasri"
    from
        {{ ref('bsdasri') }} as b3
    where
        b3.eco_organisme_siret is not null
),

grouped as (
    select
        max(
            case when companies_eo.siret is null then 0 else 1 end
        )::bool as "inscrit_sur_td",
        max(
            case when eo_agrees.siret is null then 0 else 1 end
        )::bool as "eco_organisme_agree",
        max(
            coalesce(hardcoded_eo.dans_table_eo, 0)
        )::bool as dans_table_eo,
        max(
            coalesce(companies_eo."a_entré_un_agrément_eo", 0)
        )::bool as "a_entré_un_agrément_eo",
        max(
            coalesce(companies_eo."a_choisi_profil_eo", 0)
        )::bool as "a_choisi_profil_eo",
        max(
            coalesce(bsdd_eo."visé_dans_bsdd", 0)
        )::bool as "visé_dans_bsdd",
        max(
            coalesce(bsda_eo."visé_dans_bsda", 0)
        )::bool as "visé_dans_bsda",
        max(
            coalesce(bsdasri_eo."visé_dans_bsdasri", 0)
        )::bool as "visé_dans_bsdasri",
        coalesce(
            hardcoded_eo.siret,
            eo_agrees.siret,
            companies_eo.siret,
            bsdd_eo.siret,
            bsda_eo.siret,
            bsdasri_eo.siret
        )       as siret,
        max(
            coalesce(
                hardcoded_eo.name,
                eo_agrees.nom_eco_organisme,
                companies_eo.name,
                bsdd_eo.name,
                bsda_eo.name,
                bsdasri_eo.name
            )
        )       as nom
    from
        hardcoded_eo
    full outer join
        companies_eo
        on
            hardcoded_eo.siret = companies_eo.siret
    full outer join
        eo_agrees
        on
            hardcoded_eo.siret = eo_agrees.siret
    full outer join
        bsdd_eo
        on
            hardcoded_eo.siret = bsdd_eo.siret
    full outer join
        bsda_eo
        on
            hardcoded_eo.siret = bsda_eo.siret
    full outer join
        bsdasri_eo
        on
            hardcoded_eo.siret = bsdasri_eo.siret
    group by
        coalesce(
            hardcoded_eo.siret,
            eo_agrees.siret,
            companies_eo.siret,
            bsdd_eo.siret,
            bsda_eo.siret,
            bsdasri_eo.siret
        )
),

admins as (
    select
        c2.siret,
        u2."name" as nom_contact,
        u2.email  as email_contact,
        row_number() over (
            partition by (c2.siret)
            order by
                u2.created_at desc
        )         as rn
    from
        {{ ref('company') }} as c2
    left join
        {{ ref('company_association') }} as ca2
        on
            c2.id = ca2.company_id
    left join
        {{ ref('user') }} as u2
        on
            ca2.user_id = u2.id
    where
        ca2."role" = 'ADMIN'
)

select
    grouped.*,
    admins.nom_contact   as "admin_name",
    admins.email_contact as "admin_email"
from
    grouped
left join
    admins
    on
        grouped.siret = admins.siret
where
    rn = 1
