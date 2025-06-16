{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'registry_ssd') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    and isLatest
    {% else %}
    where isLatest
    {% endif %}
)

SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toTimezone(toDateTime64("createdAt",6),'Europe/Paris')) as created_at,
    assumeNotNull(toTimezone(toDateTime64("updatedAt",6),'Europe/Paris')) as updated_at,
    toNullable(toString("importId")) as import_id,
    assumeNotNull(toBool("isLatest")) as is_latest,
    assumeNotNull(toBool("isCancelled")) as is_cancelled,
    assumeNotNull(toString("createdById")) as created_by_id,
    assumeNotNull(toString("publicId")) as public_id,
    assumeNotNull(toString("reportForCompanySiret")) as report_for_company_siret,
    assumeNotNull(toString("reportForCompanyName")) as report_for_company_name,
    assumeNotNull(toString("reportForCompanyAddress")) as report_for_company_address,
    assumeNotNull(toString("reportForCompanyCity")) as report_for_company_city,
    assumeNotNull(toString("reportForCompanyPostalCode")) as report_for_company_postal_code,
    toNullable(toString("reportAsCompanySiret")) as report_as_company_siret,
    assumeNotNull(toFloat64("weightValue")) as weight_value,
    assumeNotNull(toBool("weightIsEstimate")) as weight_is_estimate,
    toNullable(toFloat64("volume")) as volume,
    toLowCardinality(assumeNotNull(toString("wasteCode"))) as waste_code,
    toLowCardinality(toNullable(toString("wasteCodeBale"))) as waste_code_bale,
    assumeNotNull(toString("wasteDescription")) as waste_description,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("secondaryWasteCodes"),2,length("secondaryWasteCodes")-2),''))) as secondary_waste_codes,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("secondaryWasteDescriptions"),2,length("secondaryWasteDescriptions")-2),''))) as secondary_waste_descriptions,
    toNullable(toTimezone(toDateTime64("dispatchDate",6),'Europe/Paris')) as dispatch_date,
    toNullable(toTimezone(toDateTime64("useDate",6),'Europe/Paris')) as use_date,
    assumeNotNull(toTimezone(toDateTime64("processingDate",6),'Europe/Paris')) as processing_date,
    toNullable(toTimezone(toDateTime64("processingEndDate",6),'Europe/Paris')) as processing_end_date,
    toLowCardinality(assumeNotNull(replaceAll(toString("operationCode"),' ',''))) as operation_code,
    toLowCardinality(toNullable(toString("operationMode"))) as operation_mode,
    assumeNotNull(toString("product")) as product,
    assumeNotNull(toString("administrativeActReference")) as administrative_act_reference,
    toNullable(toString("destinationCompanyType")) as destination_company_type,
    toNullable(toString("destinationCompanyOrgId")) as destination_company_org_id,
    toNullable(toString("destinationCompanyName")) as destination_company_name,
    toNullable(toString("destinationCompanyAddress")) as destination_company_address,
    toNullable(toString("destinationCompanyCity")) as destination_company_city,
    toLowCardinality(toNullable(toString("destinationCompanyPostalCode"))) as destination_company_postal_code,
    toLowCardinality(toNullable(toString("destinationCompanyCountryCode"))) as destination_company_country_code
FROM {{ source('trackdechets_production', 'registry_ssd') }}
