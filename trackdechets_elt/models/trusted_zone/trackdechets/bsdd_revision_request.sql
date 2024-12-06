{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["bsdd_id"]},
        { "columns": ["created_at"]},
        { "columns": ["updated_at"]}

    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsdd_revision_request_raw') }}
),

renamed as (
    select
        id,
        "createdAt"                                       as created_at,
        "updatedAt"                                       as updated_at,
        "bsddId"                                          as bsdd_id,
        "authoringCompanyId"                              as authoring_company_id,
        "comment",
        status,
        "recipientCap"                                    as recipient_cap,
        "wasteDetailsCode"                                as waste_details_code,
        "wasteDetailsPop"                                 as waste_details_pop,
        "quantityReceived"                                as quantity_received,
        "brokerCompanyName"                               as broker_company_name,
        "brokerCompanySiret"                              as broker_company_siret,
        "brokerCompanyAddress"                            as broker_company_address,
        "brokerCompanyContact"                            as broker_company_contact,
        "brokerCompanyPhone"                              as broker_company_phone,
        "brokerCompanyMail"                               as broker_company_mail,
        "brokerReceipt"                                   as broker_receipt,
        "brokerDepartment"                                as broker_department,
        "brokerValidityLimit"                             as broker_validity_limit,
        "traderCompanyAddress"                            as trader_company_address,
        "traderCompanyContact"                            as trader_company_contact,
        "traderCompanyPhone"                              as trader_company_phone,
        "traderCompanyMail"                               as trader_company_mail,
        "traderReceipt"                                   as trader_receipt,
        "traderDepartment"                                as trader_department,
        "traderValidityLimit"                             as trader_validity_limit,
        "temporaryStorageDestinationCap"                  as temporary_storage_destination_cap,
        "traderCompanySiret"                              as trader_company_siret,
        "traderCompanyName"                               as trader_company_name,
        "wasteDetailsName"                                as waste_details_name,
        "wasteDetailsPackagingInfos"                      as waste_details_packaging_infos,
        "processingOperationDescription"                  as processing_operation_description,
        "temporaryStorageTemporaryStorerQuantityReceived" as temporary_storage_temporary_storer_quantity_received,
        "isCanceled"                                      as is_canceled,
        "destinationOperationMode"                        as destination_operation_mode,
        replace(
            "processingOperationDone", ' ', ''
        )                                                 as processing_operation_done,
        replace(
            "temporaryStorageDestinationProcessingOperation", ' ', ''
        )                                                 as temporary_storage_destination_processing_operation
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    created_at,
    updated_at,
    bsdd_id,
    authoring_company_id,
    comment,
    status,
    recipient_cap,
    waste_details_code,
    waste_details_pop,
    quantity_received,
    processing_operation_done,
    broker_company_name,
    broker_company_siret,
    broker_company_address,
    broker_company_contact,
    broker_company_phone,
    broker_company_mail,
    broker_receipt,
    broker_department,
    broker_validity_limit,
    trader_company_address,
    trader_company_contact,
    trader_company_phone,
    trader_company_mail,
    trader_receipt,
    trader_department,
    trader_validity_limit,
    temporary_storage_destination_cap,
    temporary_storage_destination_processing_operation,
    trader_company_siret,
    trader_company_name,
    waste_details_name,
    waste_details_packaging_infos,
    processing_operation_description,
    temporary_storage_temporary_storer_quantity_received,
    is_canceled,
    destination_operation_mode
from renamed
