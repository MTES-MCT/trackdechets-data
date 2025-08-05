{{
  config(
    materialized = 'table',
    query_settings = {
        "join_algorithm":"'grace_hash'",
        "grace_hash_join_initial_buckets":32
    }
    )
}}

with communes as (
    --Il y a des duplicats dans les codes communes, ici on déduplique et on ajoute les données de la commune parente
    --en cas d'absence des données pour un commune déléguée. Cela concerne très peu de cas néanmoins.
    select
        cgc.code_commune,
        max(
            coalesce(cgc.code_departement, cgc2.code_departement)
        )                                                as code_departement,
        max(coalesce(cgc.code_region, cgc2.code_region)) as code_region
    from
        {{ ref('code_geo_communes') }} as cgc
    left join
        {{ ref('code_geo_communes') }} as cgc2
        on cgc.code_commune_parente = cgc2.code_commune
    group by cgc.code_commune
)

select
    c.id,
	c.siret as "siret",
	c.updated_at,
	c.created_at,
	c.security_code,
	c.name,
	c.gerep_id,
	c.code_naf,
	c.given_name,
	c.contact_email,
	c.contact_phone,
	c.website,
	c.transporter_receipt_id,
	c.trader_receipt_id,
	c.eco_organisme_agreements,
	c.company_types,
	c.address,
	c.latitude,
	c.longitude,
	c.broker_receipt_id,
	c.verification_code,
	c.verification_status,
	c.verification_mode,
	c.verification_comment,
	c.verified_at,
	c.vhu_agrement_demolisseur_id,
	c.vhu_agrement_broyeur_id,
	c.allow_bsdasri_take_over_without_signature,
	c.vat_number,
	c.contact,
	c.code_departement as "code_departement",
	c.worker_certification_id,
	c.org_id,
	c.collector_types,
	c.waste_processor_types,
	c.webhook_settings_limit,
	c.allow_appendix1_signature_automation,
	c.feature_flags,
	c.waste_vehicles_types,
	c.is_dormant_since,
    toNullable(naf.code_section) as code_section,
    toNullable(naf.libelle_section) as libelle_section,
    toNullable(naf.code_division) as code_division,
    toNullable(naf.libelle_division) as libelle_division,
    toNullable(naf.code_groupe) as code_groupe,
    toNullable(naf.libelle_groupe) as libelle_groupe,
    toNullable(naf.code_classe) as code_classe,
    toNullable(naf.libelle_classe) as libelle_classe,
    toNullable(naf.code_sous_classe) as code_sous_classe,
    toNullable(naf.libelle_sous_classe) as libelle_sous_classe,
    etabs.etat_administratif_etablissement,
    communes.code_commune     as code_commune_insee,
    communes.code_departement as code_departement_insee,
    communes.code_region      as code_region_insee
from
    {{ ref('company') }} as c
left join
    {{ ref('stock_etablissement') }}
        as etabs
    on c.siret = etabs.siret
left join
    communes
    on
        coalesce(
            etabs.code_commune_etablissement,
            etabs.code_commune_2_etablissement
        ) = communes.code_commune
left join
    {{ ref('nomenclature_activites_francaises') }}
        as naf
    on replace(
        coalesce(etabs.activite_principale_etablissement, c.code_naf),
        '.',
        ''
    ) = replace(
        naf.code_sous_classe,
        '.',
        ''
    )
