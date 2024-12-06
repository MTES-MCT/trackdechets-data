{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["bsff_id"]},
        { "columns": ["operation_date"]}

    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsff_packaging_raw') }}
),

renamed as (
    select
        id,
        numero,
        "bsffId"                                   as bsff_id,
        "acceptationDate"                          as acceptation_date,
        "acceptationRefusalReason"                 as acceptation_refusal_reason,
        "acceptationSignatureAuthor"               as acceptation_signature_author,
        "acceptationSignatureDate"                 as acceptation_signature_date,
        "acceptationStatus"                        as acceptation_status,
        "acceptationWasteCode"                     as acceptation_waste_code,
        "acceptationWasteDescription"              as acceptation_waste_description,
        "operationDate"                            as operation_date,
        "operationDescription"                     as operation_description,
        "operationNoTraceability"                  as operation_no_traceability,
        "operationSignatureAuthor"                 as operation_signature_author,
        "operationSignatureDate"                   as operation_signature_date,
        "operationNextDestinationCap"              as operation_next_destination_cap,
        "operationNextDestinationCompanyAddress"   as operation_next_destination_company_address,
        "operationNextDestinationCompanyContact"   as operation_next_destination_company_contact,
        "operationNextDestinationCompanyMail"      as operation_next_destination_company_mail,
        "operationNextDestinationCompanyName"      as operation_next_destination_company_name,
        "operationNextDestinationCompanyPhone"     as operation_next_destination_company_phone,
        "operationNextDestinationCompanySiret"     as operation_next_destination_company_siret,
        "operationNextDestinationCompanyVatNumber" as operation_next_destination_company_vat_number,
        "nextPackagingId"                          as next_packaging_id,
        other,
        "type"                                     as type,
        "emissionNumero"                           as emission_numero,
        "operationMode"                            as operation_mode,
        volume
        / 1000                                     as volume,
        weight
        / 1000                                     as weight,
        "acceptationWeight"
        / 1000                                     as acceptation_weight,
        replace(
            "operationCode", ' ', ''
        )                                          as operation_code,
        replace(
            "operationNextDestinationPlannedOperationCode", ' ', ''
        )                                          as operation_next_destination_planned_operation_code
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    volume,
    weight,
    numero,
    bsff_id,
    acceptation_date,
    acceptation_refusal_reason,
    acceptation_signature_author,
    acceptation_signature_date,
    acceptation_status,
    acceptation_weight,
    acceptation_waste_code,
    acceptation_waste_description,
    operation_date,
    operation_code,
    operation_description,
    operation_no_traceability,
    operation_signature_author,
    operation_signature_date,
    operation_next_destination_planned_operation_code,
    operation_next_destination_cap,
    operation_next_destination_company_address,
    operation_next_destination_company_contact,
    operation_next_destination_company_mail,
    operation_next_destination_company_name,
    operation_next_destination_company_phone,
    operation_next_destination_company_siret,
    operation_next_destination_company_vat_number,
    next_packaging_id,
    other,
    type,
    emission_numero,
    operation_mode
from renamed
