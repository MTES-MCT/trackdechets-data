{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["association_id"], "unique": True},
        { "columns": ["company_id"]},
        { "columns": ["user_id"]},
        { "columns": ["company_siret"]},
    ]
    )
}}

select
    ca.id                                        as "association_id",
    ca."role"                                    as "user_role",
    u.id                                         as "user_id",
    u.email                                      as "user_email",
    u.name                                       as "user_name",
    u.phone                                      as "user_phone",
    u.created_at                                 as "user_created_at",
    u.updated_at                                 as "user_updated_at",
    u.is_active                                  as "user_is_active",
    u.activated_at                               as "user_activated_at",
    u.first_association_date                     as "user_first_association_date",
    u.is_admin                                   as "user_is_admin",
    u.is_registre_national                       as "user_is_registre_national",
    ce.id                                        as "company_id",
    ce.siret                                     as "company_siret",
    ce.created_at                                as "company_created_at",
    ce.updated_at                                as "company_updated_at",
    ce."name"                                    as "company_name",
    ce.code_naf                                  as "company_code_naf",
    ce.given_name                                as "company_given_name",
    ce.contact_email                             as "company_contact_email",
    ce.contact_phone                             as "company_contact_phone",
    ce.website                                   as "company_website",
    ce.company_types                             as "company_company_types",
    ce.address                                   as "company_address",
    ce.latitude                                  as "company_latitude",
    ce.longitude                                 as "company_longitude",
    ce.broker_receipt_id                         as "company_broker_receipt_id",
    ce.verification_code                         as "company_verification_code",
    ce.verification_status                       as "company_verification_status",
    ce.verification_mode                         as "company_verification_mode",
    ce.verification_comment                      as "company_verification_comment",
    ce.verified_at                               as "company_verified_at",
    ce.allow_bsdasri_take_over_without_signature as "company_allow_bsdasri_take_over_without_signature",
    ce.vat_number                                as "company_vat_number",
    ce.code_departement                          as "company_code_departement",
    ce.code_section                              as "company_code_section",
    ce.libelle_section                           as "company_libelle_section",
    ce.code_division                             as "company_code_division",
    ce.libelle_division                          as "company_libelle_division",
    ce.code_groupe                               as "company_code_groupe",
    ce.libelle_groupe                            as "company_libelle_groupe",
    ce.code_classe                               as "company_code_classe",
    ce.libelle_classe                            as "company_libelle_classe",
    ce.code_sous_classe                          as "company_code_sous_classe",
    ce.libelle_sous_classe                       as "company_libelle_sous_classe",
    ce.etat_administratif_etablissement          as "company_etat_administratif_etablissement",
    ce.code_commune_insee                        as "company_code_commune_insee",
    ce.code_departement_insee                    as "company_code_departement_insee",
    ce.code_region_insee                         as "company_code_region_insee"
from
    {{ ref('company_association') }} as ca
inner join
    {{ ref('company_enriched') }} as ce
    on
        ca.company_id = ce.id
inner join
    {{ ref('user') }} as u
    on
        ca.user_id = u.id
where
    "role" = 'ADMIN'
