{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsdd_transporter') }} as b
    {% if is_incremental() %}
        where
            b."updatedAt"
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
)

select
    toString(
        id
    ) as id,
    toInt16(
        "number"
    ) as number,
    toNullable(
        toString("transporterCompanySiret")
    ) as transporter_company_siret,
    toNullable(
        toString("transporterCompanyName")
    ) as transporter_company_name,
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
        toBool("transporterIsExemptedOfReceipt")
    ) as transporter_is_exempted_of_receipt,
    toNullable(
        toString("transporterReceipt")
    ) as transporter_receipt,
    toLowCardinality(
        toNullable(toString("transporterDepartment"))
    ) as transporter_department,
    toNullable(
        toTimezone(toDateTime64("transporterValidityLimit", 6), 'Europe/Paris')
    ) as transporter_validity_limit,
    toNullable(
        toString("transporterNumberPlate")
    ) as transporter_number_plate,
    toLowCardinality(
        toNullable(toString("transporterTransportMode"))
    ) as transporter_transport_mode,
    toNullable(
        toBool("readyToTakeOver")
    ) as ready_to_take_over,
    toNullable(
        toTimezone(toDateTime64("takenOverAt", 6), 'Europe/Paris')
    ) as taken_over_at,
    toNullable(
        toString("takenOverBy")
    ) as taken_over_by,
    toTimezone(
        toDateTime64("createdAt", 6), 'Europe/Paris'
    ) as created_at,
    toTimezone(
        toDateTime64("updatedAt", 6), 'Europe/Paris'
    ) as updated_at,
    toNullable(
        toString("formId")
    ) as form_id,
    toNullable(
        toString("previousTransporterCompanyOrgId")
    ) as previous_transporter_company_org_id,
    toNullable(
        toString("transporterCompanyVatNumber")
    ) as transporter_company_vat_number,
    toNullable(
        toString("transporterCustomInfo")
    ) as transporter_custom_info
from source
