{{ config(
  pre_hook = "{{ create_indexes_for_source(['formId','createdAt','updatedAt','transporterCompanySiret','takenOverAt']) }}"
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsdd_transporter_raw') }}
),

renamed as (
    select
        id,
        "number",
        "transporterCompanySiret"          as transporter_company_siret,
        "transporterCompanyName"           as transporter_company_name,
        "transporterCompanyAddress"        as transporter_company_address,
        "transporterCompanyContact"        as transporter_company_contact,
        "transporterCompanyPhone"          as transporter_company_phone,
        "transporterCompanyMail"           as transporter_company_mail,
        "transporterIsExemptedOfReceipt"   as transporter_is_exempted_of_receipt,
        "transporterReceipt"               as transporter_receipt,
        "transporterDepartment"            as transporter_department,
        cast(
            "transporterValidityLimit" as timestamptz
        )                                  as transporter_validity_limit,
        "transporterNumberPlate"           as transporter_number_plate,
        "transporterTransportMode"         as transporter_transport_mode,
        "readyToTakeOver"                  as ready_to_take_over,
        cast("takenOverAt" as timestamptz) as taken_over_at,
        "takenOverBy"                      as taken_over_by,
        "createdAt"                        as created_at,
        "updatedAt"                        as updated_at,
        "formId"                           as form_id,
        "previousTransporterCompanyOrgId"  as previous_transporter_company_org_id,
        "transporterCompanyVatNumber"      as transporter_company_vat_number,
        "transporterCustomInfo"            as transporter_custom_info
    from
        source
)

select
    id,
    "number",
    transporter_company_siret,
    transporter_company_name,
    transporter_company_address,
    transporter_company_contact,
    transporter_company_phone,
    transporter_company_mail,
    transporter_is_exempted_of_receipt,
    transporter_receipt,
    transporter_department,
    transporter_validity_limit,
    transporter_number_plate,
    transporter_transport_mode,
    ready_to_take_over,
    taken_over_at,
    taken_over_by,
    created_at,
    updated_at,
    form_id,
    previous_transporter_company_org_id,
    transporter_company_vat_number,
    transporter_custom_info
from renamed
