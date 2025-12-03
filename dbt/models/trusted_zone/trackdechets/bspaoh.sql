{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns',
    enabled=false 
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bspaoh') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)

SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toTimezone(toDateTime64("createdAt",6),'Europe/Paris')) as created_at,
    assumeNotNull(toTimezone(toDateTime64("updatedAt",6),'Europe/Paris')) as updated_at,
    assumeNotNull(toBool("isDeleted")) as is_deleted,
    toLowCardinality(assumeNotNull(toString("status"))) as status,
    toLowCardinality(toNullable(toString("wasteCode"))) as waste_code,
    toNullable(toString("wasteAdr")) as waste_adr,
    toLowCardinality(assumeNotNull(toString("wasteType"))) as waste_type,
    toNullable(toString("wastePackagings")) as waste_packagings,
    toNullable(toString("emitterCompanyName")) as emitter_company_name,
    toNullable(toString("emitterCompanySiret")) as emitter_company_siret,
    toNullable(toString("emitterCompanyAddress")) as emitter_company_address,
    toNullable(toString("emitterCompanyContact")) as emitter_company_contact,
    toNullable(toString("emitterCompanyPhone")) as emitter_company_phone,
    toNullable(toString("emitterCompanyMail")) as emitter_company_mail,
    toNullable(toString("emitterCustomInfo")) as emitter_custom_info,
    toNullable(toString("emitterPickupSiteName")) as emitter_pickup_site_name,
    toNullable(toString("emitterPickupSiteAddress")) as emitter_pickup_site_address,
    toNullable(toString("emitterPickupSiteCity")) as emitter_pickup_site_city,
    toNullable(toString("emitterPickupSitePostalCode")) as emitter_pickup_site_postal_code,
    toNullable(toString("emitterPickupSiteInfos")) as emitter_pickup_site_infos,
    toNullable(toInt256("emitterWasteQuantityValue")) as emitter_waste_quantity_value,
    toNullable(toFloat64("emitterWasteWeightValue")) as emitter_waste_weight_value,
    toNullable(toBool("emitterWasteWeightIsEstimate")) as emitter_waste_weight_is_estimate,
    toNullable(toString("emitterEmissionSignatureAuthor")) as emitter_emission_signature_author,
    toNullable(toTimezone(toDateTime64("emitterEmissionSignatureDate",6),'Europe/Paris')) as emitter_emission_signature_date,
    toNullable(toTimezone(toDateTime64("transporterTransportTakenOverAt",6),'Europe/Paris')) as transporter_transport_taken_over_at,
    toNullable(toString("destinationCompanyName")) as destination_company_name,
    toNullable(toString("destinationCompanySiret")) as destination_company_siret,
    toNullable(toString("destinationCompanyAddress")) as destination_company_address,
    toNullable(toString("destinationCompanyContact")) as destination_company_contact,
    toNullable(toString("destinationCompanyPhone")) as destination_company_phone,
    toNullable(toString("destinationCompanyMail")) as destination_company_mail,
    toNullable(toString("destinationCustomInfo")) as destination_custom_info,
    toNullable(toString("destinationCap")) as destination_cap,
    toNullable(toTimezone(toDateTime64("handedOverToDestinationSignatureDate",6),'Europe/Paris')) as handed_over_to_destination_signature_date,
    toNullable(toString("handedOverToDestinationSignatureAuthor")) as handed_over_to_destination_signature_author,
    toNullable(toInt256("destinationReceptionWasteQuantityValue")) as destination_reception_waste_quantity_value,
    toNullable(toString("destinationReceptionAcceptationStatus")) as destination_reception_acceptation_status,
    toNullable(toString("destinationReceptionWasteRefusalReason")) as destination_reception_waste_refusal_reason,
    toNullable(toString("destinationReceptionWastePackagingsAcceptation")) as destination_reception_waste_packagings_acceptation,
    toNullable(toTimezone(toDateTime64("destinationReceptionDate",6),'Europe/Paris')) as destination_reception_date,
    toNullable(toTimezone(toDateTime64("destinationReceptionSignatureDate",6),'Europe/Paris')) as destination_reception_signature_date,
    toNullable(toString("destinationReceptionSignatureAuthor")) as destination_reception_signature_author,
    toLowCardinality(toNullable(toString("destinationOperationCode"))) as destination_operation_code,
    toNullable(toTimezone(toDateTime64("destinationOperationDate",6),'Europe/Paris')) as destination_operation_date,
    toNullable(toTimezone(toDateTime64("destinationOperationSignatureDate",6),'Europe/Paris')) as destination_operation_signature_date,
    toNullable(toString("destinationOperationSignatureAuthor")) as destination_operation_signature_author,
    toNullable(toString("currentTransporterOrgId")) as current_transporter_org_id,
    toNullable(toString("nextTransporterOrgId")) as next_transporter_org_id,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("transportersSirets"),2,length("transportersSirets")-2),''))) as transporters_sirets,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("canAccessDraftSirets"),2,length("canAccessDraftSirets")-2),''))) as can_access_draft_sirets,
    assumeNotNull(toInt256("rowNumber")) as row_number,
    toNullable(toFloat64("destinationReceptionWasteAcceptedWeightValue")) as destination_reception_waste_accepted_weight_value,
    toNullable(toFloat64("destinationReceptionWasteReceivedWeightValue")) as destination_reception_waste_received_weight_value,
    toNullable(toFloat64("destinationReceptionWasteRefusedWeightValue")) as destination_reception_waste_refused_weight_value,
    toNullable(toString("isDuplicateOf")) as is_duplicate_of
 FROM source