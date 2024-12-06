{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["created_at"]},
        { "columns": ["updated_at"]},
        { "columns": ["siret"]},
        { "columns": ["org_id"]}

    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'company_raw') }}
),

renamed as (
    select
        id,
        siret,
        "updatedAt"                            as updated_at,
        "createdAt"                            as created_at,
        "securityCode"                         as security_code,
        "name",
        "gerepId"                              as gerep_id,
        "codeNaf"                              as code_naf,
        "givenName"                            as given_name,
        "contactEmail"                         as contact_email,
        "contactPhone"                         as contact_phone,
        website,
        "transporterReceiptId"                 as transporter_receipt_id,
        "traderReceiptId"                      as trader_receipt_id,
        /*Cast JSONB array to TEXT array without the " in the values*/
        address,
        latitude,
        longitude,
        "brokerReceiptId"                      as broker_receipt_id,
        "verificationCode"                     as verification_code,
        "verificationStatus"                   as verification_status,
        "verificationMode"                     as verification_mode,
        "verificationComment"                  as verification_comment,
        "verifiedAt"                           as verified_at,
        "vhuAgrementDemolisseurId"             as vhu_agrement_demolisseur_id,
        "vhuAgrementBroyeurId"                 as vhu_agrement_broyeur_id,
        "allowBsdasriTakeOverWithoutSignature" as allow_bsdasri_take_over_without_signature,
        "vatNumber"                            as vat_number,
        contact,
        "codeDepartement"                      as code_departement,
        "workerCertificationId"                as worker_certification_id,
        "orgId"                                as org_id,
        string_to_array(
            replace(
                array_to_string("ecoOrganismeAgreements", ','),
                '"',
                ''
            ),
            ','
        )                                      as eco_organisme_agreements,
        string_to_array(
            replace(
                array_to_string("companyTypes", ','),
                '"',
                ''
            ),
            ','
        )                                      as company_types,
        string_to_array(
            replace(
                array_to_string("collectorTypes", ','),
                '"',
                ''
            ),
            ','
        )                                      as collector_types,
        string_to_array(
            replace(
                array_to_string("wasteProcessorTypes", ','),
                '"',
                ''
            ),
            ','
        )                                      as waste_processor_types
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    siret,
    updated_at,
    created_at,
    security_code,
    "name",
    gerep_id,
    code_naf,
    given_name,
    contact_email,
    contact_phone,
    website,
    transporter_receipt_id,
    trader_receipt_id,
    eco_organisme_agreements,
    company_types,
    address,
    latitude,
    longitude,
    broker_receipt_id,
    verification_code,
    verification_status,
    verification_mode,
    verification_comment,
    verified_at,
    vhu_agrement_demolisseur_id,
    vhu_agrement_broyeur_id,
    allow_bsdasri_take_over_without_signature,
    vat_number,
    contact,
    code_departement,
    worker_certification_id,
    org_id,
    collector_types,
    waste_processor_types
from renamed
