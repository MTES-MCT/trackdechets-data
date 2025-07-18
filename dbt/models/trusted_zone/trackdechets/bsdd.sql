{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'bsdd') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toTimezone(toDateTime64("createdAt",6),'Europe/Paris')) as created_at,
    assumeNotNull(toTimezone(toDateTime64("updatedAt",6),'Europe/Paris')) as updated_at,
    toLowCardinality(toNullable(toString("emitterType"))) as emitter_type,
    toNullable(toString("emitterPickupSite")) as emitter_pickup_site,
    toNullable(toString("emitterCompanyName")) as emitter_company_name,
    toNullable(toString("emitterCompanySiret")) as emitter_company_siret,
    toNullable(toString("emitterCompanyAddress")) as emitter_company_address,
    toNullable(toString("emitterCompanyContact")) as emitter_company_contact,
    toNullable(toString("emitterCompanyPhone")) as emitter_company_phone,
    toNullable(toString("emitterCompanyMail")) as emitter_company_mail,
    toNullable(toString("recipientCap")) as recipient_cap,
    toLowCardinality(toNullable(replaceAll(toString("recipientProcessingOperation"),' ',''))) as recipient_processing_operation,
    toNullable(toString("recipientCompanyName")) as recipient_company_name,
    toNullable(toString("recipientCompanySiret")) as recipient_company_siret,
    toNullable(toString("recipientCompanyAddress")) as recipient_company_address,
    toNullable(toString("recipientCompanyContact")) as recipient_company_contact,
    toNullable(toString("recipientCompanyPhone")) as recipient_company_phone,
    toNullable(toString("recipientCompanyMail")) as recipient_company_mail,
    toLowCardinality(toNullable(toString("wasteDetailsCode"))) as waste_details_code,
    toNullable(toString("wasteDetailsOnuCode")) as waste_details_onu_code,
    toNullable(toDecimal256("wasteDetailsQuantity", 30)) as waste_details_quantity,
    toLowCardinality(toNullable(toString("wasteDetailsQuantityType"))) as waste_details_quantity_type,
    assumeNotNull(toString("readableId")) as readable_id,
    toLowCardinality(assumeNotNull(toString("status"))) as status,
    toNullable(toTimezone(toDateTime64("sentAt",6),'Europe/Paris')) as sent_at,
    toNullable(toString("sentBy")) as sent_by,
    toNullable(toBool("isAccepted")) as is_accepted,
    toNullable(toTimezone(toDateTime64("receivedAt",6),'Europe/Paris')) as received_at,
    toNullable(toDecimal256("quantityReceived", 30)) as quantity_received,
    toLowCardinality(toNullable(replaceAll(toString("processingOperationDone"),' ',''))) as processing_operation_done,
    toNullable(toString("wasteDetailsName")) as waste_details_name,
    toNullable(toBool("isDeleted")) as is_deleted,
    toNullable(toString("receivedBy")) as received_by,
    toLowCardinality(toNullable(toString("wasteDetailsConsistence"))) as waste_details_consistence,
    toNullable(toString("processedBy")) as processed_by,
    toNullable(toTimezone(toDateTime64("processedAt",6),'Europe/Paris')) as processed_at,
    toLowCardinality(toNullable(replaceAll(toString("nextDestinationProcessingOperation"),' ',''))) as next_destination_processing_operation,
    toNullable(toString("traderCompanyName")) as trader_company_name,
    toNullable(toString("traderCompanySiret")) as trader_company_siret,
    toNullable(toString("traderCompanyAddress")) as trader_company_address,
    toNullable(toString("traderCompanyContact")) as trader_company_contact,
    toNullable(toString("traderCompanyPhone")) as trader_company_phone,
    toNullable(toString("traderCompanyMail")) as trader_company_mail,
    toNullable(toString("traderReceipt")) as trader_receipt,
    toNullable(toString("traderDepartment")) as trader_department,
    toNullable(toTimezone(toDateTime64("traderValidityLimit",6),'Europe/Paris')) as trader_validity_limit,
    toNullable(toString("processingOperationDescription")) as processing_operation_description,
    toNullable(toBool("noTraceability")) as no_traceability,
    toNullable(toBool("signedByTransporter")) as signed_by_transporter,
    toNullable(toString("customId")) as custom_id,
    toLowCardinality(toNullable(toString("wasteAcceptationStatus"))) as waste_acceptation_status,
    toNullable(toString("wasteRefusalReason")) as waste_refusal_reason,
    toNullable(toString("nextDestinationCompanyName")) as next_destination_company_name,
    toNullable(toString("nextDestinationCompanySiret")) as next_destination_company_siret,
    toNullable(toString("nextDestinationCompanyAddress")) as next_destination_company_address,
    toNullable(toString("nextDestinationCompanyContact")) as next_destination_company_contact,
    toNullable(toString("nextDestinationCompanyPhone")) as next_destination_company_phone,
    toNullable(toString("nextDestinationCompanyMail")) as next_destination_company_mail,
    toNullable(toString("emitterWorkSiteName")) as emitter_work_site_name,
    toNullable(toString("emitterWorkSiteAddress")) as emitter_work_site_address,
    toNullable(toString("emitterWorkSiteCity")) as emitter_work_site_city,
    toLowCardinality(toNullable(toString("emitterWorkSitePostalCode"))) as emitter_work_site_postal_code,
    toNullable(toString("emitterWorkSiteInfos")) as emitter_work_site_infos,
    toNullable(toBool("recipientIsTempStorage")) as recipient_is_temp_storage,
    toNullable(toTimezone(toDateTime64("signedAt",6),'Europe/Paris')) as signed_at,
    toNullable(toString("currentTransporterOrgId")) as current_transporter_org_id,
    toNullable(toString("nextTransporterOrgId")) as next_transporter_org_id,
    toNullable(toString("nextDestinationCompanyCountry")) as next_destination_company_country,
    assumeNotNull(toBool("isImportedFromPaper")) as is_imported_from_paper,
    toNullable(toString("ecoOrganismeName")) as eco_organisme_name,
    toNullable(toString("ecoOrganismeSiret")) as eco_organisme_siret,
    assumeNotNull(toString("wasteDetailsPackagingInfos")) as waste_details_packaging_infos,
    toNullable(toString("signedBy")) as signed_by,
    assumeNotNull(toBool("wasteDetailsPop")) as waste_details_pop,
    assumeNotNull(toString("ownerId")) as owner_id,
    toNullable(toString("brokerCompanyName")) as broker_company_name,
    toNullable(toString("brokerCompanySiret")) as broker_company_siret,
    toNullable(toString("brokerCompanyAddress")) as broker_company_address,
    toNullable(toString("brokerCompanyContact")) as broker_company_contact,
    toNullable(toString("brokerCompanyPhone")) as broker_company_phone,
    toNullable(toString("brokerCompanyMail")) as broker_company_mail,
    toNullable(toString("brokerReceipt")) as broker_receipt,
    toLowCardinality(toNullable(toString("brokerDepartment"))) as broker_department,
    toNullable(toTimezone(toDateTime64("brokerValidityLimit",6),'Europe/Paris')) as broker_validity_limit,
    assumeNotNull(toBool("wasteDetailsIsDangerous")) as waste_details_is_dangerous,
    toNullable(toTimezone(toDateTime64("emittedAt",6),'Europe/Paris')) as emitted_at,
    toNullable(toString("emittedBy")) as emitted_by,
    toNullable(toBool("emittedByEcoOrganisme")) as emitted_by_eco_organisme,
    toNullable(toTimezone(toDateTime64("takenOverAt",6),'Europe/Paris')) as taken_over_at,
    toNullable(toString("takenOverBy")) as taken_over_by,
    toNullable(toString("wasteDetailsParcelNumbers")) as waste_details_parcel_numbers,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("wasteDetailsAnalysisReferences"),2,length("wasteDetailsAnalysisReferences")-2),''))) as waste_details_analysis_references,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("wasteDetailsLandIdentifiers"),2,length("wasteDetailsLandIdentifiers")-2),''))) as waste_details_land_identifiers,
    toNullable(toString("forwardedInId")) as forwarded_in_id,
    toNullable(toString("quantityReceivedType")) as quantity_received_type,
    toNullable(toBool("emitterIsForeignShip")) as emitter_is_foreign_ship,
    toNullable(toBool("emitterIsPrivateIndividual")) as emitter_is_private_individual,
    toNullable(toString("emitterCompanyOmiNumber")) as emitter_company_omi_number,
    toNullable(toString("nextDestinationCompanyVatNumber")) as next_destination_company_vat_number,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("recipientsSirets"),2,length("recipientsSirets")-2),''))) as recipients_sirets,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("transportersSirets"),2,length("transportersSirets")-2),''))) as transporters_sirets,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("intermediariesSirets"),2,length("intermediariesSirets")-2),''))) as intermediaries_sirets,
    toNullable(toString("nextDestinationNotificationNumber")) as next_destination_notification_number,
    toNullable(toString("wasteDetailsSampleNumber")) as waste_details_sample_number,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("canAccessDraftSirets"),2,length("canAccessDraftSirets")-2),''))) as can_access_draft_sirets,
    toLowCardinality(toNullable(toString("destinationOperationMode"))) as destination_operation_mode,
    assumeNotNull(toFloat64("quantityGrouped")) as quantity_grouped,
    toNullable(toString("nextDestinationCompanyExtraEuropeanId")) as next_destination_company_extra_european_id,
    assumeNotNull(toInt256("rowNumber")) as row_number,
    toNullable(toDecimal256("quantityRefused", 30)) as quantity_refused,
    toNullable(toString("citerneNotWashedOutReason")) as citerne_not_washed_out_reason,
    toNullable(toBool("hasCiterneBeenWashedOut")) as has_citerne_been_washed_out,
    toNullable(toString("emptyReturnADR")) as empty_return_adr,
    toNullable(toString("wasteDetailsNonRoadRegulationMention")) as waste_details_non_road_regulation_mention,
    toNullable(toBool("wasteDetailsIsSubjectToADR")) as waste_details_is_subject_to_adr
 FROM source