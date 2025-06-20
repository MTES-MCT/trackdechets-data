{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsff_transporter') }} b
    {% if is_incremental() %}
    where b."updatedAt" >= (SELECT toString(toStartOfDay(max(updated_at)))  FROM {{ this }})
    {% endif %}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toTimezone(toDateTime64("createdAt",6),'Europe/Paris')) as created_at,
    assumeNotNull(toTimezone(toDateTime64("updatedAt",6),'Europe/Paris')) as updated_at,
    assumeNotNull(toInt256("number")) as number,
    toNullable(toString("bsffId")) as bsff_id,
    toNullable(toString("transporterCompanySiret")) as transporter_company_siret,
    toNullable(toString("transporterCompanyName")) as transporter_company_name,
    toNullable(toString("transporterCompanyVatNumber")) as transporter_company_vat_number,
    toNullable(toString("transporterCompanyAddress")) as transporter_company_address,
    toNullable(toString("transporterCompanyContact")) as transporter_company_contact,
    toNullable(toString("transporterCompanyPhone")) as transporter_company_phone,
    toNullable(toString("transporterCompanyMail")) as transporter_company_mail,
    toNullable(toString("transporterCustomInfo")) as transporter_custom_info,
    toNullable(toBool("transporterRecepisseIsExempted")) as transporter_recepisse_is_exempted,
    toNullable(toString("transporterRecepisseNumber")) as transporter_recepisse_number,
    toLowCardinality(toNullable(toString("transporterRecepisseDepartment"))) as transporter_recepisse_department,
    toNullable(toTimezone(toDateTime64("transporterRecepisseValidityLimit",6),'Europe/Paris')) as transporter_recepisse_validity_limit,
    toLowCardinality(toNullable(toString("transporterTransportMode"))) as transporter_transport_mode,
    assumeNotNull(splitByChar(',',COALESCE (substring(toString("transporterTransportPlates"),2,length("transporterTransportPlates")-2),''))) as transporter_transport_plates,
    toNullable(toTimezone(toDateTime64("transporterTransportTakenOverAt",6),'Europe/Paris')) as transporter_transport_taken_over_at,
    toNullable(toString("transporterTransportSignatureAuthor")) as transporter_transport_signature_author,
    toNullable(toTimezone(toDateTime64("transporterTransportSignatureDate",6),'Europe/Paris')) as transporter_transport_signature_date
 FROM source