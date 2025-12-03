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
    from
        {{ source('trackdechets_production', 'bsdasri_revision_request') }} as b
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
        toDateTime64("createdAt", 6)
    ) as created_at,
    assumeNotNull(
        toDateTime64("updatedAt", 6)
    ) as updated_at,
    toLowCardinality(
        assumeNotNull(toString("status"))
    ) as status,
    assumeNotNull(
        toString("comment")
    ) as comment,
    assumeNotNull(
        toBool("isCanceled")
    ) as is_canceled,
    assumeNotNull(
        toString("bsdasriId")
    ) as bsdasri_id,
    assumeNotNull(
        toString("authoringCompanyId")
    ) as authoring_company_id,
    toLowCardinality(
        toNullable(toString("wasteCode"))
    ) as waste_code,
    toNullable(
        toString("destinationWastePackagings")
    ) as destination_waste_packagings,
    toNullable(
        toFloat64("destinationReceptionWasteWeightValue")
    ) as destination_reception_waste_weight_value,
    toLowCardinality(
        toNullable(replaceAll(toString("destinationOperationCode"), ' ', ''))
    ) as destination_operation_code,
    toLowCardinality(
        toNullable(toString("destinationOperationMode"))
    ) as destination_operation_mode,
    toNullable(
        toString("emitterPickupSiteName")
    ) as emitter_pickup_site_name,
    toNullable(
        toString("emitterPickupSiteAddress")
    ) as emitter_pickup_site_address,
    toNullable(
        toString("emitterPickupSiteCity")
    ) as emitter_pickup_site_city,
    toNullable(
        toString("emitterPickupSitePostalCode")
    ) as emitter_pickup_site_postal_code,
    toNullable(
        toString("emitterPickupSiteInfos")
    ) as emitter_pickup_site_infos,
    toLowCardinality(
        toNullable(
            replaceAll(toString("initialDestinationOperationCode"), ' ', '')
        )
    ) as initial_destination_operation_code,
    toLowCardinality(
        toNullable(toString("initialDestinationOperationMode"))
    ) as initial_destination_operation_mode,
    toNullable(
        toDecimal256("initialDestinationReceptionWasteWeightValue", 30)
    ) as initial_destination_reception_waste_weight_value,
    toNullable(
        toString("initialDestinationWastePackagings")
    ) as initial_destination_waste_packagings,
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
    toNullable(
        toString("initialEmitterPickupSitePostalCode")
    ) as initial_emitter_pickup_site_postal_code,
    toLowCardinality(
        toNullable(toString("initialWasteCode"))
    ) as initial_waste_code
from source
