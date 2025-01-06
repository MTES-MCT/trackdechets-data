{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsda_transporter') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT 
    toString("id") AS id,
    toDateTime64("createdAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("createdAt",'Europe/Paris')) AS created_at,
    toDateTime64("updatedAt", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("updatedAt",'Europe/Paris')) AS updated_at,
    toInt256("number") AS number,
    toNullable(toString("bsdaId")) AS bsda_id,
    toNullable(toString("transporterCompanySiret")) AS transporter_company_siret,
    toNullable(toString("transporterCompanyName")) AS transporter_company_name,
    toNullable(toString("transporterCompanyVatNumber")) AS transporter_company_vat_number,
    toNullable(toString("transporterCompanyAddress")) AS transporter_company_address,
    toNullable(toString("transporterCompanyContact")) AS transporter_company_contact,
    toNullable(toString("transporterCompanyPhone")) AS transporter_company_phone,
    toNullable(toString("transporterCompanyMail")) AS transporter_company_mail,
    toNullable(toString("transporterCustomInfo")) AS transporter_custom_info,
    toNullable(toBool("transporterRecepisseIsExempted")) AS transporter_recepisse_is_exempted,
    toNullable(toString("transporterRecepisseNumber")) AS transporter_recepisse_number,
    toLowCardinality(toNullable(toString("transporterRecepisseDepartment"))) AS transporter_recepisse_department,
    toNullable(toDateTime64("transporterRecepisseValidityLimit", 6, 'Europe/Paris')) - timeZoneOffset(toTimeZone("transporterRecepisseValidityLimit",'Europe/Paris')) AS transporter_recepisse_validity_limit,
    toLowCardinality(toNullable(toString("transporterTransportMode"))) AS transporter_transport_mode,
    toNullable(toString("transporterTransportPlates")) AS transporter_transport_plates,
    toNullable(toDateTime64("transporterTransportTakenOverAt", 6, 'Europe/Paris')) - timeZoneOffset(toTimeZone("transporterTransportTakenOverAt",'Europe/Paris')) AS transporter_transport_taken_over_at,
    toNullable(toString("transporterTransportSignatureAuthor")) AS transporter_transport_signature_author,
    toNullable(toDateTime64("transporterTransportSignatureDate", 6, 'Europe/Paris')) - timeZoneOffset(toTimeZone("transporterTransportSignatureDate",'Europe/Paris')) AS transporter_transport_signature_date
FROM source
