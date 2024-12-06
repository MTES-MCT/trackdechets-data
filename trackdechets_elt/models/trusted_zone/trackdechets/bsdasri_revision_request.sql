with source as (
    select *
    from {{ source('trackdechets_production', 'bsdasri_revision_request_raw') }}
),

renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("createdAt") }}                            as created_at,
        {{ adapter.quote("updatedAt") }}                            as "updated_at",
        {{ adapter.quote("status") }},
        {{ adapter.quote("comment") }},
        {{ adapter.quote("isCanceled") }}                           as "is_canceled",
        {{ adapter.quote("bsdasriId") }}                            as "bsdasri_id",
        {{ adapter.quote("authoringCompanyId") }}                   as "authoring_company_id",
        {{ adapter.quote("wasteCode") }},
        {{ adapter.quote("destinationWastePackagings") }}           as "destination_waste_packagings",
        {{ adapter.quote("destinationReceptionWasteWeightValue") }} as "destination_reception_waste_weight_value",
        {{ adapter.quote("destinationOperationCode") }}             as "destination_operation_code",
        {{ adapter.quote("destinationOperationMode") }}             as "destination_operation_mode",
        {{ adapter.quote("emitterPickupSiteName") }}                as "emitter_pickup_site_name",
        {{ adapter.quote("emitterPickupSiteAddress") }}             as "emitter_pickup_site_address",
        {{ adapter.quote("emitterPickupSiteCity") }}                as "emitter_pickup_site_city",
        {{ adapter.quote("emitterPickupSitePostalCode") }}          as "emitter_pickup_site_postal_code",
        {{ adapter.quote("emitterPickupSiteInfos") }}               as "emitter_pickup_site_infos"

    from source
)

select * from renamed
