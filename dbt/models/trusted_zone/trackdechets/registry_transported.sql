{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'registry_transported') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    and is_latest
    {% else %}
    where is_latest
    {% endif %}
)

SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toDateTime64("createdAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("createdAt",'Europe/Paris'))) as created_at,
    toNullable(toString("importId")) as import_id,
    assumeNotNull(toBool("isLatest")) as is_latest,
    assumeNotNull(toBool("isCancelled")) as is_cancelled,
    assumeNotNull(toString("createdById")) as created_by_id,
    assumeNotNull(toString("publicId")) as public_id,
    assumeNotNull(toString("reportForCompanySiret")) as report_for_company_siret,
    assumeNotNull(toString("reportForCompanyName")) as report_for_company_name,
    assumeNotNull(toString("reportForCompanyAddress")) as report_for_company_address,
    assumeNotNull(toString("reportForCompanyCity")) as report_for_company_city,
    toLowCardinality(assumeNotNull(toString("reportForCompanyPostalCode"))) as report_for_company_postal_code,
    toLowCardinality(assumeNotNull(toString("reportForTransportMode"))) as report_for_transport_mode,
    assumeNotNull(toBool("reportForTransportIsWaste")) as report_for_transport_is_waste,
    toNullable(toBool("reportForRecepisseIsExempted")) as report_for_recepisse_is_exempted,
    toNullable(toString("reportForRecepisseNumber")) as report_for_recepisse_number,
    toNullable(toString("reportForTransportAdr")) as report_for_transport_adr,
    toNullable(toString("reportForTransportOtherTmdCode")) as report_for_transport_other_tmd_code,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("reportForTransportPlates"),2,length("reportForTransportPlates")-2),''))) as report_for_transport_plates,
    toNullable(toString("reportAsCompanySiret")) as report_as_company_siret,
    assumeNotNull(toString("wasteDescription")) as waste_description,
    toLowCardinality(toNullable(toString("wasteCode"))) as waste_code,
    toLowCardinality(toNullable(toString("wasteCodeBale"))) as waste_code_bale,
    toNullable(toBool("wastePop")) as waste_pop,
    toNullable(toBool("wasteIsDangerous")) as waste_is_dangerous,
    assumeNotNull(toDateTime64("collectionDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("collectionDate",'Europe/Paris'))) as collection_date,
    assumeNotNull(toDateTime64("unloadingDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("unloadingDate",'Europe/Paris'))) as unloading_date,
    assumeNotNull(toFloat64("weightValue")) as weight_value,
    assumeNotNull(toBool("weightIsEstimate")) as weight_is_estimate,
    toNullable(toFloat64("volume")) as volume,
    assumeNotNull(toString("emitterCompanyType")) as emitter_company_type,
    toNullable(toString("emitterCompanyOrgId")) as emitter_company_org_id,
    toNullable(toString("emitterCompanyName")) as emitter_company_name,
    toNullable(toString("emitterCompanyAddress")) as emitter_company_address,
    toLowCardinality(toNullable(toString("emitterCompanyPostalCode"))) as emitter_company_postal_code,
    toNullable(toString("emitterCompanyCity")) as emitter_company_city,
    toLowCardinality(toNullable(toString("emitterCompanyCountryCode"))) as emitter_company_country_code,
    toNullable(toString("emitterPickupSiteName")) as emitter_pickup_site_name,
    toNullable(toString("emitterPickupSiteAddress")) as emitter_pickup_site_address,
    toLowCardinality(toNullable(toString("emitterPickupSitePostalCode"))) as emitter_pickup_site_postal_code,
    toNullable(toString("emitterPickupSiteCity")) as emitter_pickup_site_city,
    toLowCardinality(toNullable(toString("emitterPickupSiteCountryCode"))) as emitter_pickup_site_country_code,
    assumeNotNull(toString("destinationCompanyType")) as destination_company_type,
    toNullable(toString("destinationCompanyOrgId")) as destination_company_org_id,
    toNullable(toString("destinationCompanyName")) as destination_company_name,
    toNullable(toString("destinationCompanyAddress")) as destination_company_address,
    toLowCardinality(toNullable(toString("destinationCompanyPostalCode"))) as destination_company_postal_code,
    toNullable(toString("destinationCompanyCity")) as destination_company_city,
    toLowCardinality(toNullable(toString("destinationCompanyCountryCode"))) as destination_company_country_code,
    toNullable(toString("destinationDropSiteAddress")) as destination_drop_site_address,
    toLowCardinality(toNullable(toString("destinationDropSitePostalCode"))) as destination_drop_site_postal_code,
    toNullable(toString("destinationDropSiteCity")) as destination_drop_site_city,
    toLowCardinality(toNullable(toString("destinationDropSiteCountryCode"))) as destination_drop_site_country_code,
    toNullable(toString("movementNumber")) as movement_number,
    toNullable(toString("ecoOrganismeSiret")) as eco_organisme_siret,
    toNullable(toString("ecoOrganismeName")) as eco_organisme_name,
    toNullable(toString("brokerCompanySiret")) as broker_company_siret,
    toNullable(toString("brokerCompanyName")) as broker_company_name,
    toNullable(toString("brokerRecepisseNumber")) as broker_recepisse_number,
    toNullable(toString("traderCompanySiret")) as trader_company_siret,
    toNullable(toString("traderCompanyName")) as trader_company_name,
    toNullable(toString("traderRecepisseNumber")) as trader_recepisse_number,
    toNullable(toString("gistridNumber")) as gistrid_number,
    assumeNotNull(toDateTime64("updatedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("updatedAt",'Europe/Paris'))) as updated_at
 FROM source