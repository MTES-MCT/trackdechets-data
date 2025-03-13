{{
  config(
    materialized = 'table'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'bsff_packaging') }} b
)
SELECT
    assumeNotNull(toString("id")) as id,
    toNullable(toFloat64("volume"))/1000 as volume,
    assumeNotNull(toFloat64("weight"))/1000 as weight,
    assumeNotNull(toString("numero")) as numero,
    assumeNotNull(toString("bsffId")) as bsff_id,
    toNullable(toDateTime64("acceptationDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("acceptationDate",'Europe/Paris'))) as acceptation_date,
    toNullable(toString("acceptationRefusalReason")) as acceptation_refusal_reason,
    toNullable(toString("acceptationSignatureAuthor")) as acceptation_signature_author,
    toNullable(toDateTime64("acceptationSignatureDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("acceptationSignatureDate",'Europe/Paris'))) as acceptation_signature_date,
    toLowCardinality(toNullable(toString("acceptationStatus"))) as acceptation_status,
    toNullable(toFloat64("acceptationWeight"))/1000 as acceptation_weight,
    toLowCardinality(toNullable(toString("acceptationWasteCode"))) as acceptation_waste_code,
    toNullable(toString("acceptationWasteDescription")) as acceptation_waste_description,
    toNullable(toDateTime64("operationDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("operationDate",'Europe/Paris'))) as operation_date,
    toLowCardinality(toNullable(replaceAll(toString("operationCode"),' ',''))) as operation_code,
    toNullable(toString("operationDescription")) as operation_description,
    assumeNotNull(toBool("operationNoTraceability")) as operation_no_traceability,
    toNullable(toString("operationSignatureAuthor")) as operation_signature_author,
    toNullable(toDateTime64("operationSignatureDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("operationSignatureDate",'Europe/Paris'))) as operation_signature_date,
    toNullable(replaceAll(toString("operationNextDestinationPlannedOperationCode"),' ','')) as operation_next_destination_planned_operation_code,
    toNullable(toString("operationNextDestinationCap")) as operation_next_destination_cap,
    toNullable(toString("operationNextDestinationCompanyAddress")) as operation_next_destination_company_address,
    toNullable(toString("operationNextDestinationCompanyContact")) as operation_next_destination_company_contact,
    toNullable(toString("operationNextDestinationCompanyMail")) as operation_next_destination_company_mail,
    toNullable(toString("operationNextDestinationCompanyName")) as operation_next_destination_company_name,
    toNullable(toString("operationNextDestinationCompanyPhone")) as operation_next_destination_company_phone,
    toNullable(toString("operationNextDestinationCompanySiret")) as operation_next_destination_company_siret,
    toNullable(toString("operationNextDestinationCompanyVatNumber")) as operation_next_destination_company_vat_number,
    toNullable(toString("nextPackagingId")) as next_packaging_id,
    toNullable(toString("other")) as other,
    toLowCardinality(assumeNotNull(toString("type"))) as type,
    assumeNotNull(toString("emissionNumero")) as emission_numero,
    toLowCardinality(toNullable(toString("operationMode"))) as operation_mode
 FROM source
