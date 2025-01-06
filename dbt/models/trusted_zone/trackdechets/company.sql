{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'company') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT
    assumeNotNull(toString("id")) as id,
    toNullable(toString("siret")) as siret,
    assumeNotNull(toDateTime64("updatedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("updatedAt",'Europe/Paris'))) as updated_at,
    assumeNotNull(toDateTime64("createdAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("createdAt",'Europe/Paris'))) as created_at,
    assumeNotNull(toInt256("securityCode")) as security_code,
    assumeNotNull(toString("name")) as name,
    toNullable(toString("gerepId")) as gerep_id,
    toLowCardinality(toNullable(toString("codeNaf"))) as code_naf,
    toNullable(toString("givenName")) as given_name,
    toNullable(toString("contactEmail")) as contact_email,
    toNullable(toString("contactPhone")) as contact_phone,
    toNullable(toString("website")) as website,
    toNullable(toString("transporterReceiptId")) as transporter_receipt_id,
    toNullable(toString("traderReceiptId")) as trader_receipt_id,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("ecoOrganismeAgreements"),2,length("ecoOrganismeAgreements")-2),''))) as eco_organisme_agreements,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("companyTypes"),2,length("companyTypes")-2),''))) as company_types,
    toNullable(toString("address")) as address,
    toNullable(toFloat64("latitude")) as latitude,
    toNullable(toFloat64("longitude")) as longitude,
    toNullable(toString("brokerReceiptId")) as broker_receipt_id,
    assumeNotNull(toString("verificationCode")) as verification_code,
    toLowCardinality(assumeNotNull(toString("verificationStatus"))) as verification_status,
    toLowCardinality(toNullable(toString("verificationMode"))) as verification_mode,
    toNullable(toString("verificationComment")) as verification_comment,
    toNullable(toDateTime64("verifiedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("verifiedAt",'Europe/Paris'))) as verified_at,
    toNullable(toString("vhuAgrementDemolisseurId")) as vhu_agrement_demolisseur_id,
    toNullable(toString("vhuAgrementBroyeurId")) as vhu_agrement_broyeur_id,
    assumeNotNull(toBool("allowBsdasriTakeOverWithoutSignature")) as allow_bsdasri_take_over_without_signature,
    toNullable(toString("vatNumber")) as vat_number,
    toNullable(toString("contact")) as contact,
    toLowCardinality(toNullable(toString("codeDepartement"))) as code_departement,
    toNullable(toString("workerCertificationId")) as worker_certification_id,
    assumeNotNull(toString("orgId")) as org_id,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("collectorTypes"),2,length("collectorTypes")-2),''))) as collector_types,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("wasteProcessorTypes"),2,length("wasteProcessorTypes")-2),''))) as waste_processor_types,
    assumeNotNull(toInt256("webhookSettingsLimit")) as webhook_settings_limit,
    assumeNotNull(toBool("allowAppendix1SignatureAutomation")) as allow_appendix1_signature_automation,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("featureFlags"),2,length("featureFlags")-2),''))) as feature_flags,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("wasteVehiclesTypes"),2,length("wasteVehiclesTypes")-2),''))) as waste_vehicles_types,
    toNullable(toDateTime64("isDormantSince", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("isDormantSince",'Europe/Paris'))) as is_dormant_since
 FROM source