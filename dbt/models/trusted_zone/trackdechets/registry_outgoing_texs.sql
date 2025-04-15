{{
  config(
    materialized = 'table',
    enabled=false
    )
}}

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
    assumeNotNull(toString("reportForCompanyPostalCode")) as report_for_company_postal_code,
    toNullable(toString("reportAsCompanySiret")) as report_as_company_siret,
    toNullable(toString("reportForPickupSiteName")) as report_for_pickup_site_name,
    toNullable(toString("reportForPickupSiteAddress")) as report_for_pickup_site_address,
    toNullable(toString("reportForPickupSitePostalCode")) as report_for_pickup_site_postal_code,
    toNullable(toString("reportForPickupSiteCity")) as report_for_pickup_site_city,
    toNullable(toString("reportForPickupSiteCountryCode")) as report_for_pickup_site_country_code,
    assumeNotNull(toString("wasteDescription")) as waste_description,
    toLowCardinality(toNullable(toString("wasteCode"))) as waste_code,
    toLowCardinality(toNullable(toString("wasteCodeBale"))) as waste_code_bale,
    assumeNotNull(toBool("wastePop")) as waste_pop,
    toNullable(toBool("wasteIsDangerous")) as waste_is_dangerous,
    assumeNotNull(toDateTime64("dispatchDate", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("dispatchDate",'Europe/Paris'))) as dispatch_date,
    toNullable(toString("wasteDap")) as waste_dap,
    assumeNotNull(toFloat64("weightValue")) as weight_value,
    assumeNotNull(toBool("weightIsEstimate")) as weight_is_estimate,
    toNullable(toFloat64("volume")) as volume,
    toNullable(toString("initialEmitterCompanyType")) as initial_emitter_company_type,
    toNullable(toString("initialEmitterCompanyOrgId")) as initial_emitter_company_org_id,
    toNullable(toString("initialEmitterCompanyName")) as initial_emitter_company_name,
    toNullable(toString("initialEmitterCompanyAddress")) as initial_emitter_company_address,
    toLowCardinality(toNullable(toString("initialEmitterCompanyPostalCode"))) as initial_emitter_company_postal_code,
    toNullable(toString("initialEmitterCompanyCity")) as initial_emitter_company_city,
    toLowCardinality(toNullable(toString("initialEmitterCompanyCountryCode"))) as initial_emitter_company_country_code,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("initialEmitterMunicipalitiesInseeCodes"),2,length("initialEmitterMunicipalitiesInseeCodes")-2),''))) as initial_emitter_municipalities_insee_codes,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("parcelInseeCodes"),2,length("parcelInseeCodes")-2),''))) as parcel_insee_codes,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("parcelNumbers"),2,length("parcelNumbers")-2),''))) as parcel_numbers,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("parcelCoordinates"),2,length("parcelCoordinates")-2),''))) as parcel_coordinates,
    toNullable(toString("sisIdentifier")) as sis_identifier,
    assumeNotNull(toString("destinationCompanyType")) as destination_company_type,
    toNullable(toString("destinationCompanyOrgId")) as destination_company_org_id,
    toNullable(toString("destinationCompanyName")) as destination_company_name,
    toNullable(toString("destinationCompanyAddress")) as destination_company_address,
    toNullable(toString("destinationCompanyCity")) as destination_company_city,
    toNullable(toString("destinationCompanyPostalCode")) as destination_company_postal_code,
    toNullable(toString("destinationCompanyCountryCode")) as destination_company_country_code,
    toNullable(toString("destinationDropSiteAddress")) as destination_drop_site_address,
    toNullable(toString("destinationDropSitePostalCode")) as destination_drop_site_postal_code,
    toNullable(toString("destinationDropSiteCity")) as destination_drop_site_city,
    toNullable(toString("destinationDropSiteCountryCode")) as destination_drop_site_country_code,
    toLowCardinality(assumeNotNull(toString("operationCode"))) as operation_code,
    toLowCardinality(toNullable(toString("operationMode"))) as operation_mode,
    toNullable(toBool("isUpcycled")) as is_upcycled,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("destinationParcelInseeCodes"),2,length("destinationParcelInseeCodes")-2),''))) as destination_parcel_insee_codes,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("destinationParcelNumbers"),2,length("destinationParcelNumbers")-2),''))) as destination_parcel_numbers,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("destinationParcelCoordinates"),2,length("destinationParcelCoordinates")-2),''))) as destination_parcel_coordinates,
    toNullable(toString("movementNumber")) as movement_number,
    toNullable(toString("ecoOrganismeSiret")) as eco_organisme_siret,
    toNullable(toString("ecoOrganismeName")) as eco_organisme_name,
    toNullable(toString("brokerCompanySiret")) as broker_company_siret,
    toNullable(toString("brokerCompanyName")) as broker_company_name,
    toNullable(toString("brokerRecepisseNumber")) as broker_recepisse_number,
    toNullable(toString("traderCompanySiret")) as trader_company_siret,
    toNullable(toString("traderCompanyName")) as trader_company_name,
    toNullable(toString("traderRecepisseNumber")) as trader_recepisse_number,
    toNullable(toBool("isDirectSupply")) as is_direct_supply,
    toLowCardinality(toNullable(toString("transporter1TransportMode"))) as transporter1_transport_mode,
    toLowCardinality(toNullable(toString("transporter1CompanyType"))) as transporter1_company_type,
    toNullable(toString("transporter1CompanyOrgId")) as transporter1_company_org_id,
    toNullable(toBool("transporter1RecepisseIsExempted")) as transporter1_recepisse_is_exempted,
    toNullable(toString("transporter1RecepisseNumber")) as transporter1_recepisse_number,
    toNullable(toString("transporter1CompanyName")) as transporter1_company_name,
    toNullable(toString("transporter1CompanyAddress")) as transporter1_company_address,
    toLowCardinality(toNullable(toString("transporter1CompanyPostalCode"))) as transporter1_company_postal_code,
    toNullable(toString("transporter1CompanyCity")) as transporter1_company_city,
    toLowCardinality(toNullable(toString("transporter1CompanyCountryCode"))) as transporter1_company_country_code,
    toLowCardinality(toNullable(toString("transporter2TransportMode"))) as transporter2_transport_mode,
    toLowCardinality(toNullable(toString("transporter2CompanyType"))) as transporter2_company_type,
    toNullable(toString("transporter2CompanyOrgId")) as transporter2_company_org_id,
    toNullable(toBool("transporter2RecepisseIsExempted")) as transporter2_recepisse_is_exempted,
    toNullable(toString("transporter2RecepisseNumber")) as transporter2_recepisse_number,
    toNullable(toString("transporter2CompanyName")) as transporter2_company_name,
    toNullable(toString("transporter2CompanyAddress")) as transporter2_company_address,
    toLowCardinality(toNullable(toString("transporter2CompanyPostalCode"))) as transporter2_company_postal_code,
    toNullable(toString("transporter2CompanyCity")) as transporter2_company_city,
    toLowCardinality(toNullable(toString("transporter2CompanyCountryCode"))) as transporter2_company_country_code,
    toLowCardinality(toNullable(toString("transporter3TransportMode"))) as transporter3_transport_mode,
    toLowCardinality(toNullable(toString("transporter3CompanyType"))) as transporter3_company_type,
    toNullable(toString("transporter3CompanyOrgId")) as transporter3_company_org_id,
    toNullable(toBool("transporter3RecepisseIsExempted")) as transporter3_recepisse_is_exempted,
    toNullable(toString("transporter3RecepisseNumber")) as transporter3_recepisse_number,
    toNullable(toString("transporter3CompanyName")) as transporter3_company_name,
    toNullable(toString("transporter3CompanyAddress")) as transporter3_company_address,
    toLowCardinality(toNullable(toString("transporter3CompanyPostalCode"))) as transporter3_company_postal_code,
    toNullable(toString("transporter3CompanyCity")) as transporter3_company_city,
    toLowCardinality(toNullable(toString("transporter3CompanyCountryCode"))) as transporter3_company_country_code,
    toLowCardinality(toNullable(toString("transporter4TransportMode"))) as transporter4_transport_mode,
    toLowCardinality(toNullable(toString("transporter4CompanyType"))) as transporter4_company_type,
    toNullable(toString("transporter4CompanyOrgId")) as transporter4_company_org_id,
    toNullable(toBool("transporter4RecepisseIsExempted")) as transporter4_recepisse_is_exempted,
    toNullable(toString("transporter4RecepisseNumber")) as transporter4_recepisse_number,
    toNullable(toString("transporter4CompanyName")) as transporter4_company_name,
    toNullable(toString("transporter4CompanyAddress")) as transporter4_company_address,
    toLowCardinality(toNullable(toString("transporter4CompanyPostalCode"))) as transporter4_company_postal_code,
    toNullable(toString("transporter4CompanyCity")) as transporter4_company_city,
    toLowCardinality(toNullable(toString("transporter4CompanyCountryCode"))) as transporter4_company_country_code,
    toLowCardinality(toNullable(toString("transporter5TransportMode"))) as transporter5_transport_mode,
    toLowCardinality(toNullable(toString("transporter5CompanyType"))) as transporter5_company_type,
    toNullable(toString("transporter5CompanyOrgId")) as transporter5_company_org_id,
    toNullable(toBool("transporter5RecepisseIsExempted")) as transporter5_recepisse_is_exempted,
    toNullable(toString("transporter5RecepisseNumber")) as transporter5_recepisse_number,
    toNullable(toString("transporter5CompanyName")) as transporter5_company_name,
    toNullable(toString("transporter5CompanyAddress")) as transporter5_company_address,
    toLowCardinality(toNullable(toString("transporter5CompanyPostalCode"))) as transporter5_company_postal_code,
    toNullable(toString("transporter5CompanyCity")) as transporter5_company_city,
    toLowCardinality(toNullable(toString("transporter5CompanyCountryCode"))) as transporter5_company_country_code,
    toNullable(toString("gistridNumber")) as gistrid_number
FROM {{ source('trackdechets', 'registry_outgoing_texs') }}
WHERE "isLatest"
