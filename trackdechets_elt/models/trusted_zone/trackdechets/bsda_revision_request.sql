{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True},
        { "columns": ["bsda_id"]},
        { "columns": ["created_at"]},
        { "columns": ["updated_at"]}

    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsda_revision_request_raw') }}
),

renamed as (
    select
        id,
        "createdAt"                       as created_at,
        "updatedAt"                       as updated_at,
        "bsdaId"                          as bsda_id,
        "authoringCompanyId"              as authoring_company_id,
        "comment",
        status,
        "wasteCode"                       as waste_code,
        "wastePop"                        as waste_pop,
        packagings,
        "wasteSealNumbers"                as waste_seal_numbers,
        "wasteMaterialName"               as waste_material_name,
        "destinationCap"                  as destination_cap,
        "destinationOperationDescription" as destination_operation_description,
        "brokerCompanyName"               as broker_company_name,
        "brokerCompanySiret"              as broker_company_siret,
        "brokerCompanyAddress"            as broker_company_address,
        "brokerCompanyContact"            as broker_company_contact,
        "brokerCompanyPhone"              as broker_company_phone,
        "brokerCompanyMail"               as broker_company_mail,
        "brokerRecepisseNumber"           as broker_recepisse_number,
        "brokerRecepisseDepartment"       as broker_recepisse_department,
        "brokerRecepisseValidityLimit"    as broker_recepisse_validity_limit,
        "emitterPickupSiteName"           as emitter_pickup_site_name,
        "emitterPickupSiteAddress"        as emitter_pickup_site_address,
        "emitterPickupSiteCity"           as emitter_pickup_site_city,
        "emitterPickupSitePostalCode"     as emitter_pickup_site_postal_code,
        "emitterPickupSiteInfos"          as emitter_pickup_site_infos,
        "isCanceled"                      as is_canceled,
        "destinationOperationMode"        as destination_operation_mode,
        "destinationReceptionWeight"
        / 1000                            as destination_reception_weight,
        replace(
            "destinationOperationCode", ' ', ''
        )                                 as destination_operation_code
    from
        source
    where _sdc_sync_started_at >= (select max(_sdc_sync_started_at) from source)
)

select
    id,
    created_at,
    updated_at,
    bsda_id,
    authoring_company_id,
    comment,
    status,
    waste_code,
    waste_pop,
    packagings,
    waste_seal_numbers,
    waste_material_name,
    destination_cap,
    destination_reception_weight,
    destination_operation_code,
    destination_operation_description,
    broker_company_name,
    broker_company_siret,
    broker_company_address,
    broker_company_contact,
    broker_company_phone,
    broker_company_mail,
    broker_recepisse_number,
    broker_recepisse_department,
    broker_recepisse_validity_limit,
    emitter_pickup_site_name,
    emitter_pickup_site_address,
    emitter_pickup_site_city,
    emitter_pickup_site_postal_code,
    emitter_pickup_site_infos,
    is_canceled,
    destination_operation_mode
from renamed
