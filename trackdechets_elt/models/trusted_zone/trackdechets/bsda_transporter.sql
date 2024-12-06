{{ config(
  pre_hook = "{{ create_indexes_for_source(['bsdaId','createdAt','updatedAt','transporterCompanySiret','transporterTransportTakenOverAt']) }}"
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsda_transporter_raw') }}
),

renamed as (
    select
        id,
        "createdAt"                           as created_at,
        "updatedAt"                           as updated_at,
        "number"                              as number,
        "bsdaId"                              as bsda_id,
        "transporterCompanySiret"             as transporter_company_siret,
        "transporterCompanyName"              as transporter_company_name,
        "transporterCompanyVatNumber"         as transporter_company_vat_number,
        "transporterCompanyAddress"           as transporter_company_address,
        "transporterCompanyContact"           as transporter_company_contact,
        "transporterCompanyPhone"             as transporter_company_phone,
        "transporterCompanyMail"              as transporter_company_mail,
        "transporterCustomInfo"               as transporter_custom_info,
        "transporterRecepisseIsExempted"      as transporter_recepisse_is_exempted,
        "transporterRecepisseNumber"          as transporter_recepisse_number,
        "transporterRecepisseDepartment"      as transporter_recepisse_department,
        "transporterRecepisseValidityLimit"   as transporter_recepisse_validity_limit,
        "transporterTransportMode"            as transporter_transport_mode,
        "transporterTransportPlates"          as transporter_transport_plates,
        "transporterTransportTakenOverAt"     as transporter_transport_taken_over_at,
        "transporterTransportSignatureAuthor" as transporter_transport_signature_author,
        "transporterTransportSignatureDate"   as transporter_transport_signature_date
    from source
)

select
    id,
    created_at,
    updated_at,
    number,
    bsda_id,
    transporter_company_siret,
    transporter_company_name,
    transporter_company_vat_number,
    transporter_company_address,
    transporter_company_contact,
    transporter_company_phone,
    transporter_company_mail,
    transporter_custom_info,
    transporter_recepisse_is_exempted,
    transporter_recepisse_number,
    transporter_recepisse_department,
    transporter_recepisse_validity_limit,
    transporter_transport_mode,
    transporter_transport_plates,
    transporter_transport_taken_over_at,
    transporter_transport_signature_author,
    transporter_transport_signature_date
from renamed
