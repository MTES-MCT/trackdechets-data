{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'bsda_revision_request') }} as b
    {% if is_incremental() %}
        where
            b."updatedAt"
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
)

select
    assumeNotNull(
        toString("id")
    ) as id,
    assumeNotNull(
        toTimeZone(toDateTime64("createdAt", 6), 'UTC')
    ) as created_at,
    assumeNotNull(
        toTimeZone(toDateTime64("updatedAt", 6), 'UTC')
    ) as updated_at,
    assumeNotNull(
        toString("bsdaId")
    ) as bsda_id,
    assumeNotNull(
        toString("authoringCompanyId")
    ) as authoring_company_id,
    assumeNotNull(
        toString("comment")
    ) as comment,
    toLowCardinality(
        assumeNotNull(toString("status"))
    ) as status,
    toLowCardinality(
        toNullable(toString("wasteCode"))
    ) as waste_code,
    toNullable(
        toBool("wastePop")
    ) as waste_pop,
    toNullable(
        toString("packagings")
    ) as packagings,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("wasteSealNumbers"),
                    2,
                    length("wasteSealNumbers") - 2
                ),
                ''
            )
        )
    ) as waste_seal_numbers,
    toNullable(
        toString("wasteMaterialName")
    ) as waste_material_name,
    toNullable(
        toString("destinationCap")
    ) as destination_cap,
    toNullable(
        toFloat64("destinationReceptionWeight")
    ) as destination_reception_weight,
    toLowCardinality(
        toNullable(replaceAll(toString("destinationOperationCode"), ' ', ''))
    ) as destination_operation_code,
    toNullable(
        toString("destinationOperationDescription")
    ) as destination_operation_description,
    toNullable(
        toString("brokerCompanyName")
    ) as broker_company_name,
    toNullable(
        toString("brokerCompanySiret")
    ) as broker_company_siret,
    toNullable(
        toString("brokerCompanyAddress")
    ) as broker_company_address,
    toNullable(
        toString("brokerCompanyContact")
    ) as broker_company_contact,
    toNullable(
        toString("brokerCompanyPhone")
    ) as broker_company_phone,
    toNullable(
        toString("brokerCompanyMail")
    ) as broker_company_mail,
    toNullable(
        toString("brokerRecepisseNumber")
    ) as broker_recepisse_number,
    toLowCardinality(
        toNullable(toString("brokerRecepisseDepartment"))
    ) as broker_recepisse_department,
    toNullable(
        toTimezone(
            toDateTime64("brokerRecepisseValidityLimit", 6), 'Europe/Paris'
        )
    ) as broker_recepisse_validity_limit,
    toNullable(
        toString("emitterPickupSiteName")
    ) as emitter_pickup_site_name,
    toNullable(
        toString("emitterPickupSiteAddress")
    ) as emitter_pickup_site_address,
    toNullable(
        toString("emitterPickupSiteCity")
    ) as emitter_pickup_site_city,
    toLowCardinality(
        toNullable(toString("emitterPickupSitePostalCode"))
    ) as emitter_pickup_site_postal_code,
    toNullable(
        toString("emitterPickupSiteInfos")
    ) as emitter_pickup_site_infos,
    assumeNotNull(
        toBool("isCanceled")
    ) as is_canceled,
    toLowCardinality(
        toNullable(toString("destinationOperationMode"))
    ) as destination_operation_mode,
    toNullable(
        toString("initialBrokerCompanyAddress")
    ) as initial_broker_company_address,
    toNullable(
        toString("initialBrokerCompanyContact")
    ) as initial_broker_company_contact,
    toNullable(
        toString("initialBrokerCompanyMail")
    ) as initial_broker_company_mail,
    toNullable(
        toString("initialBrokerCompanyName")
    ) as initial_broker_company_name,
    toNullable(
        toString("initialBrokerCompanyPhone")
    ) as initial_broker_company_phone,
    toNullable(
        toString("initialBrokerCompanySiret")
    ) as initial_broker_company_siret,
    toLowCardinality(
        toNullable(toString("initialBrokerRecepisseDepartment"))
    ) as initial_broker_recepisse_department,
    toNullable(
        toString("initialBrokerRecepisseNumber")
    ) as initial_broker_recepisse_number,
    toNullable(
        toTimezone(
            toDateTime64("initialBrokerRecepisseValidityLimit", 6),
            'Europe/Paris'
        )
    ) as initial_broker_recepisse_validity_limit,
    toNullable(
        toString("initialDestinationCap")
    ) as initial_destination_cap,
    toLowCardinality(
        toNullable(
            replaceAll(toString("initialDestinationOperationCode"), ' ', '')
        )
    ) as initial_destination_operation_code,
    toNullable(
        toString("initialDestinationOperationDescription")
    ) as initial_destination_operation_description,
    toLowCardinality(
        toNullable(toString("initialDestinationOperationMode"))
    ) as initial_destination_operation_mode,
    toNullable(
        toDecimal256("initialDestinationReceptionWeight", 30)
    ) as initial_destination_reception_weight,
    toNullable(
        toString("initialEmitterPickupSiteAddress")
    ) as initial_emitter_pickup_site_address,
    toNullable(
        toString("initialEmitterPickupSiteCity")
    ) as initial_emitter_pickup_site_city,
    toNullable(
        toString("initialEmitterPickupSiteInfos")
    ) as initial_emitter_pickup_site_infos,
    toNullable(
        toString("initialEmitterPickupSiteName")
    ) as initial_emitter_pickup_site_name,
    toLowCardinality(
        toNullable(toString("initialEmitterPickupSitePostalCode"))
    ) as initial_emitter_pickup_site_postal_code,
    toNullable(
        toString("initialPackagings")
    ) as initial_packagings,
    toLowCardinality(
        toNullable(toString("initialWasteCode"))
    ) as initial_waste_code,
    toNullable(
        toString("initialWasteMaterialName")
    ) as initial_waste_material_name,
    toNullable(
        toBool("initialWastePop")
    ) as initial_waste_pop,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("initialWasteSealNumbers"),
                    2,
                    length("initialWasteSealNumbers") - 2
                ),
                ''
            )
        )
    ) as initial_waste_seal_numbers
from source
