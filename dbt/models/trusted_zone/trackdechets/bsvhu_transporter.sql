{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsvhu_transporter') }} as b
    {% if is_incremental() %}
        where
            b."updatedAt"
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
)

select
    toString(
        "id"
    ) as id,
    toTimezone(
        toDateTime64("createdAt", 6), 'Europe/Paris'
    ) as created_at,
    toTimezone(
        toDateTime64("updatedAt", 6), 'Europe/Paris'
    ) as updated_at,
    toInt256(
        "number"
    ) as number,
    toNullable(
        toString("bsvhuId")
    ) as bsvhu_id,
    toNullable(
        toString("transporterCompanySiret")
    ) as transporter_company_siret,
    toNullable(
        toString("transporterCompanyName")
    ) as transporter_company_name,
    toNullable(
        toString("transporterCompanyVatNumber")
    ) as transporter_company_vat_number,
    toNullable(
        toString("transporterCompanyAddress")
    ) as transporter_company_address,
    toNullable(
        toString("transporterCompanyContact")
    ) as transporter_company_contact,
    toNullable(
        toString("transporterCompanyPhone")
    ) as transporter_company_phone,
    toNullable(
        toString("transporterCompanyMail")
    ) as transporter_company_mail,
    toNullable(
        toString("transporterCustomInfo")
    ) as transporter_custom_info,
    toNullable(
        toBool("transporterRecepisseIsExempted")
    ) as transporter_recepisse_is_exempted,
    toNullable(
        toString("transporterRecepisseNumber")
    ) as transporter_recepisse_number,
    toLowCardinality(
        toNullable(toString("transporterRecepisseDepartment"))
    ) as transporter_recepisse_department,
    toNullable(
        toTimezone(
            toDateTime64("transporterRecepisseValidityLimit", 6), 'Europe/Paris'
        )
    ) as transporter_recepisse_validity_limit,
    toLowCardinality(
        toNullable(toString("transporterTransportMode"))
    ) as transporter_transport_mode,
    toNullable(
        toString("transporterTransportPlates")
    ) as transporter_transport_plates,
    toNullable(
        toTimeZone(
            toDateTime64("transporterTransportTakenOverAt", 6), 'Europe/Paris'
        )
    ) as transporter_transport_taken_over_at,
    toNullable(
        toString("transporterTransportSignatureAuthor")
    ) as transporter_transport_signature_author,
    toNullable(
        toTimeZone(
            toDateTime64("transporterTransportSignatureDate", 6), 'Europe/Paris'
        )
    ) as transporter_transport_signature_date
from source
