{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsff') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toTimezone(toDateTime64("createdAt",6),'Europe/Paris')) as created_at,
    assumeNotNull(toTimezone(toDateTime64("updatedAt",6),'Europe/Paris')) as updated_at,
    assumeNotNull(toBool("isDeleted")) as is_deleted,
    toNullable(toString("emitterCompanyName")) as emitter_company_name,
    toNullable(toString("emitterCompanySiret")) as emitter_company_siret,
    toNullable(toString("emitterCompanyAddress")) as emitter_company_address,
    toNullable(toString("emitterCompanyContact")) as emitter_company_contact,
    toNullable(toString("emitterCompanyPhone")) as emitter_company_phone,
    toNullable(toString("emitterCompanyMail")) as emitter_company_mail,
    toNullable(toString("emitterEmissionSignatureAuthor")) as emitter_emission_signature_author,
    toNullable(toTimezone(toDateTime64("emitterEmissionSignatureDate",6),'Europe/Paris')) as emitter_emission_signature_date,
    toLowCardinality(toNullable(toString("wasteCode"))) as waste_code,
    toNullable(toString("wasteAdr")) as waste_adr,
    toNullable(toDecimal256("weightValue", 30)) /1000 as weight_value,
    toNullable(toBool("weightIsEstimate")) as weight_is_estimate,
    toNullable(toTimezone(toDateTime64("transporterTransportSignatureDate",6),'Europe/Paris')) as transporter_transport_signature_date,
    toNullable(toString("destinationCompanyName")) as destination_company_name,
    toNullable(toString("destinationCompanySiret")) as destination_company_siret,
    toNullable(toString("destinationCompanyAddress")) as destination_company_address,
    toNullable(toString("destinationCompanyContact")) as destination_company_contact,
    toNullable(toString("destinationCompanyPhone")) as destination_company_phone,
    toNullable(toString("destinationCompanyMail")) as destination_company_mail,
    toNullable(toTimezone(toDateTime64("destinationReceptionDate",6),'Europe/Paris')) as destination_reception_date,
    toNullable(toString("destinationReceptionSignatureAuthor")) as destination_reception_signature_author,
    toNullable(toTimezone(toDateTime64("destinationReceptionSignatureDate",6),'Europe/Paris')) as destination_reception_signature_date,
    toLowCardinality(toNullable(replaceAll(toString("destinationPlannedOperationCode"),' ',''))) as destination_planned_operation_code,
    toLowCardinality(assumeNotNull(toString("status"))) as status,
    toNullable(toString("wasteDescription")) as waste_description,
    assumeNotNull(toBool("isDraft")) as is_draft,
    toLowCardinality(assumeNotNull(toString("type"))) as type,
    toNullable(toString("destinationCustomInfo")) as destination_custom_info,
    toNullable(toString("emitterCustomInfo")) as emitter_custom_info,
    toNullable(toString("destinationCap")) as destination_cap,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("detenteurCompanySirets"),2,length("detenteurCompanySirets")-2),''))) as detenteur_company_sirets,
    assumeNotNull(toInt256("rowNumber")) as row_number,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("transportersOrgIds"),2,length("transportersOrgIds")-2),''))) as transporters_org_ids
 FROM source