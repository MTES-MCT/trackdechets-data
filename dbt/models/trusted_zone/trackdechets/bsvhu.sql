{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('trackdechets_production', 'bsvhu') }} as b
    {% if is_incremental() %}
        where
            b."updatedAt"
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
),

first_transporter as (
    select 
        "bsvhuId" as bsvhu_id,
        min(
            toNullable(
                toTimezone(
                    toDateTime64("transporterTransportTakenOverAt", 6), 'Europe/Paris'
                )
            )
        ) as transporter_transport_taken_over_at
    from {{ source('trackdechets_production', 'bsvhu_transporter') }} as bsvhu
    group by "bsvhuId"
)

select
    assumeNotNull(
        toString("id")
    )      as id,
    assumeNotNull(
        toTimezone(toDateTime64("createdAt", 6), 'Europe/Paris')
    )      as created_at,
    assumeNotNull(
        toTimezone(toDateTime64("updatedAt", 6), 'Europe/Paris')
    )      as updated_at,
    assumeNotNull(
        toBool("isDraft")
    )      as is_draft,
    assumeNotNull(
        toBool("isDeleted")
    )      as is_deleted,
    toLowCardinality(
        assumeNotNull(toString("status"))
    )      as status,
    toNullable(
        toString("emitterAgrementNumber")
    )      as emitter_agrement_number,
    toNullable(
        toString("emitterCompanyName")
    )      as emitter_company_name,
    toNullable(
        toString("emitterCompanySiret")
    )      as emitter_company_siret,
    toNullable(
        toString("emitterCompanyAddress")
    )      as emitter_company_address,
    toNullable(
        toString("emitterCompanyContact")
    )      as emitter_company_contact,
    toNullable(
        toString("emitterCompanyPhone")
    )      as emitter_company_phone,
    toNullable(
        toString("emitterCompanyMail")
    )      as emitter_company_mail,
    toLowCardinality(
        toNullable(toString("destinationType"))
    )      as destination_type,
    toLowCardinality(
        toNullable(
            replaceAll(toString("destinationPlannedOperationCode"), ' ', '')
        )
    )      as destination_planned_operation_code,
    toNullable(
        toString("destinationAgrementNumber")
    )      as destination_agrement_number,
    toNullable(
        toString("destinationCompanyName")
    )      as destination_company_name,
    toNullable(
        toString("destinationCompanySiret")
    )      as destination_company_siret,
    toNullable(
        toString("destinationCompanyAddress")
    )      as destination_company_address,
    toNullable(
        toString("destinationCompanyContact")
    )      as destination_company_contact,
    toNullable(
        toString("destinationCompanyPhone")
    )      as destination_company_phone,
    toNullable(
        toString("destinationCompanyMail")
    )      as destination_company_mail,
    toLowCardinality(
        toNullable(toString("wasteCode"))
    )      as waste_code,
    toNullable(
        toString("packaging")
    )      as packaging,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("identificationNumbers"),
                    2,
                    length("identificationNumbers") - 2
                ),
                ''
            )
        )
    )      as identification_numbers,
    toLowCardinality(
        toNullable(toString("identificationType"))
    )      as identification_type,
    toNullable(
        toInt256("quantity")
    )      as quantity,
    toNullable(toFloat64("weightValue"))
    / 1000 as weight_value,
    toNullable(
        toString("emitterEmissionSignatureAuthor")
    )      as emitter_emission_signature_author,
    toNullable(
        toTimezone(
            toDateTime64("emitterEmissionSignatureDate", 6), 'Europe/Paris'
        )
    )      as emitter_emission_signature_date,    
    toLowCardinality(
        toNullable(toString("destinationReceptionAcceptationStatus"))
    )      as destination_reception_acceptation_status,
    toNullable(
        toString("destinationReceptionRefusalReason")
    )      as destination_reception_refusal_reason,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("destinationReceptionIdentificationNumbers"),
                    2,
                    length("destinationReceptionIdentificationNumbers") - 2
                ),
                ''
            )
        )
    )      as destination_reception_identification_numbers,
    toNullable(
        toString("destinationReceptionIdentificationType")
    )      as destination_reception_identification_type,
    toLowCardinality(
        toNullable(replaceAll(toString("destinationOperationCode"), ' ', ''))
    )      as destination_operation_code,
    toNullable(
        toString("destinationOperationNextDestinationCompanyName")
    )      as destination_operation_next_destination_company_name,
    toNullable(
        toString("destinationOperationNextDestinationCompanySiret")
    )      as destination_operation_next_destination_company_siret,
    toNullable(
        toString("destinationOperationNextDestinationCompanyAddress")
    )      as destination_operation_next_destination_company_address,
    toNullable(
        toString("destinationOperationNextDestinationCompanyContact")
    )      as destination_operation_next_destination_company_contact,
    toNullable(
        toString("destinationOperationNextDestinationCompanyPhone")
    )      as destination_operation_next_destination_company_phone,
    toNullable(
        toString("destinationOperationNextDestinationCompanyMail")
    )      as destination_operation_next_destination_company_mail,
    toNullable(
        toString("destinationOperationSignatureAuthor")
    )      as destination_operation_signature_author,
    toNullable(
        toTimezone(
            toDateTime64("destinationOperationSignatureDate", 6), 'Europe/Paris'
        )
    )      as destination_operation_signature_date,    
    toNullable(
        toTimezone(toDateTime64("destinationOperationDate", 6), 'Europe/Paris')
    )      as destination_operation_date,
    toNullable(
        toInt256("destinationReceptionQuantity")
    )      as destination_reception_quantity,
    toNullable(toFloat64("destinationReceptionWeight"))
    / 1000 as destination_reception_weight,
    toNullable(
        toTimezone(toDateTime64("destinationReceptionDate", 6), 'Europe/Paris')
    )      as destination_reception_date,
    toNullable(
        toBool("weightIsEstimate")
    )      as weight_is_estimate,
    toNullable(
        toString("destinationOperationNextDestinationCompanyVatNumber")
    )      as destination_operation_next_destination_company_vat_number,
    toNullable(
        toString("emitterCustomInfo")
    )      as emitter_custom_info,
    toNullable(
        toString("destinationCustomInfo")
    )      as destination_custom_info,    
    toLowCardinality(
        toNullable(toString("destinationOperationMode"))
    )      as destination_operation_mode,
    assumeNotNull(
        toInt256("rowNumber")
    )      as row_number,
    toNullable(
        toBool("emitterIrregularSituation")
    )      as emitter_irregular_situation,
    toNullable(
        toBool("emitterNoSiret")
    )      as emitter_no_siret,
    toNullable(
        toString("emitterCompanyCity")
    )      as emitter_company_city,
    toNullable(
        toString("emitterCompanyCountry")
    )      as emitter_company_country,
    toLowCardinality(
        toNullable(toString("emitterCompanyPostalCode"))
    )      as emitter_company_postal_code,
    toNullable(
        toString("emitterCompanyStreet")
    )      as emitter_company_street,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("intermediariesOrgIds"),
                    2,
                    length("intermediariesOrgIds") - 2
                ),
                ''
            )
        )
    )      as intermediaries_org_ids,
    toNullable(
        toString("ecoOrganismeName")
    )      as eco_organisme_name,
    toNullable(
        toString("ecoOrganismeSiret")
    )      as eco_organisme_siret,
    toNullable(
        toString("brokerCompanyAddress")
    )      as broker_company_address,
    toNullable(
        toString("brokerCompanyContact")
    )      as broker_company_contact,
    toNullable(
        toString("brokerCompanyMail")
    )      as broker_company_mail,
    toNullable(
        toString("brokerCompanyName")
    )      as broker_company_name,
    toNullable(
        toString("brokerCompanyPhone")
    )      as broker_company_phone,
    toNullable(
        toString("brokerCompanySiret")
    )      as broker_company_siret,
    toLowCardinality(
        toNullable(toString("brokerRecepisseDepartment"))
    )      as broker_recepisse_department,
    toNullable(
        toString("brokerRecepisseNumber")
    )      as broker_recepisse_number,
    toNullable(
        toTimezone(
            toDateTime64("brokerRecepisseValidityLimit", 6), 'Europe/Paris'
        )
    )      as broker_recepisse_validity_limit,
    toNullable(
        toString("traderCompanyAddress")
    )      as trader_company_address,
    toNullable(
        toString("traderCompanyContact")
    )      as trader_company_contact,
    toNullable(
        toString("traderCompanyMail")
    )      as trader_company_mail,
    toNullable(
        toString("traderCompanyName")
    )      as trader_company_name,
    toNullable(
        toString("traderCompanyPhone")
    )      as trader_company_phone,
    toNullable(
        toString("traderCompanySiret")
    )      as trader_company_siret,
    toLowCardinality(
        toNullable(toString("traderRecepisseDepartment"))
    )      as trader_recepisse_department,
    toNullable(
        toString("traderRecepisseNumber")
    )      as trader_recepisse_number,
    toNullable(
        toTimezone(
            toDateTime64("traderRecepisseValidityLimit", 6), 'Europe/Paris'
        )
    )      as trader_recepisse_validity_limit,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("canAccessDraftOrgIds"),
                    2,
                    length("canAccessDraftOrgIds") - 2
                ),
                ''
            )
        )
    )      as can_access_draft_org_ids,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString("transportersOrgIds"),
                    2,
                    length("transportersOrgIds") - 2
                ),
                ''
            )
        )
    )      as transporters_org_ids,
    toNullable(
        toString("customId")
    )      as custom_id,

    first_transporter.transporter_transport_taken_over_at as first_transporter_transport_taken_over_at
from source
join first_transporter on source.id = first_transporter.bsvhu_id