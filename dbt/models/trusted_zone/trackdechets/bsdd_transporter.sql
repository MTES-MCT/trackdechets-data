{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsdd_transporter') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT 
    toString(id) AS id,
    toInt16("number") AS number,
    toNullable(toString("transporterCompanySiret")) AS transporter_company_siret,
    toNullable(toString("transporterCompanyName")) AS transporter_company_name,
    toNullable(toString("transporterCompanyAddress")) AS transporter_company_address,
    toNullable(toString("transporterCompanyContact")) AS transporter_company_contact,
    toNullable(toString("transporterCompanyPhone")) AS transporter_company_phone,
    toNullable(toString("transporterCompanyMail")) AS transporter_company_mail,
    toNullable(toBool("transporterIsExemptedOfReceipt")) AS transporter_is_exempted_of_receipt,
    toNullable(toString("transporterReceipt")) AS transporter_receipt,
    toLowCardinality(toNullable(toString("transporterDepartment"))) AS transporter_department,
    toNullable(toDateTime64("transporterValidityLimit", 6, 'Europe/Paris')) - timeZoneOffset(toTimeZone("transporterValidityLimit",'Europe/Paris')) AS transporter_validity_limit,
    toNullable(toString("transporterNumberPlate")) AS transporter_number_plate,
    toLowCardinality(toNullable(toString("transporterTransportMode"))) AS transporter_transport_mode,
    toNullable(toBool("readyToTakeOver")) AS ready_to_take_over,
    toNullable(toDateTime64("takenOverAt", 6, 'Europe/Paris')) - timeZoneOffset(toTimeZone("takenOverAt",'Europe/Paris')) AS taken_over_at,
    toNullable(toString("takenOverBy")) AS taken_over_by,
    toDateTime64("createdAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("createdAt",'Europe/Paris')) AS created_at,
    toDateTime64("updatedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("updatedAt",'Europe/Paris')) AS updated_at,
    toNullable(toString("formId")) AS form_id,
    toNullable(toString("previousTransporterCompanyOrgId")) AS previous_transporter_company_org_id,
    toNullable(toString("transporterCompanyVatNumber")) AS transporter_company_vat_number,
    toNullable(toString("transporterCustomInfo")) AS transporter_custom_info
FROM source