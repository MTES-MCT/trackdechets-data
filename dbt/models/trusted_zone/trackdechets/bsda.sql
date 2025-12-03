{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['id'],
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'bsda') }} as b
    {% if is_incremental() %}
        where
            b."updatedAt"
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
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
    assumeNotNull(
        toString("type")
    )      as type,
    toNullable(
        toBool("emitterIsPrivateIndividual")
    )      as emitter_is_private_individual,
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
    toNullable(
        toString("emitterPickupSiteName")
    )      as emitter_pickup_site_name,
    toNullable(
        toString("emitterPickupSiteAddress")
    )      as emitter_pickup_site_address,
    toNullable(
        toString("emitterPickupSiteCity")
    )      as emitter_pickup_site_city,
    toLowCardinality(
        toNullable(toString("emitterPickupSitePostalCode"))
    )      as emitter_pickup_site_postal_code,
    toNullable(
        toString("emitterPickupSiteInfos")
    )      as emitter_pickup_site_infos,
    toNullable(
        toString("emitterEmissionSignatureAuthor")
    )      as emitter_emission_signature_author,
    toNullable(
        toTimezone(
            toDateTime64("emitterEmissionSignatureDate", 6), 'Europe/Paris'
        )
    )      as emitter_emission_signature_date,
    toLowCardinality(
        toNullable(toString("wasteCode"))
    )      as waste_code,
    toNullable(
        toString("wasteFamilyCode")
    )      as waste_family_code,
    toNullable(
        toString("wasteMaterialName")
    )      as waste_material_name,
    toNullable(
        toString("wasteConsistence")
    )      as waste_consistence,
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
    )      as waste_seal_numbers,
    toNullable(
        toString("wasteAdr")
    )      as waste_adr,
    assumeNotNull(
        toString("packagings")
    )      as packagings,
    toNullable(toDecimal256("weightValue", 30))
    / 1000 as weight_value,
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
    toNullable(
        toString("destinationCap")
    )      as destination_cap,
    toNullable(
        replaceAll(toString("destinationPlannedOperationCode"), ' ', '')
    )      as destination_planned_operation_code,
    toNullable(
        toTimezone(toDateTime64("destinationReceptionDate", 6), 'Europe/Paris')
    )      as destination_reception_date,
    toNullable(toDecimal256("destinationReceptionWeight", 30))
    / 1000 as destination_reception_weight,
    toNullable(
        toString("destinationReceptionAcceptationStatus")
    )      as destination_reception_acceptation_status,
    toNullable(
        toString("destinationReceptionRefusalReason")
    )      as destination_reception_refusal_reason,
    toNullable(
        toDecimal256("destinationReceptionRefusedWeight", 30))
    / 1000 as destination_reception_refused_weight,
    toLowCardinality(
        toNullable(replaceAll(toString("destinationOperationCode"), ' ', ''))
    )      as destination_operation_code,
    toNullable(
        toTimezone(toDateTime64("destinationOperationDate", 6), 'Europe/Paris')
    )      as destination_operation_date,
    toNullable(
        toString("destinationOperationSignatureAuthor")
    )      as destination_operation_signature_author,
    toNullable(
        toTimezone(
            toDateTime64("destinationOperationSignatureDate", 6), 'Europe/Paris'
        )
    )      as destination_operation_signature_date,
    toNullable(
        toTimezone(
            toDateTime64("transporterTransportSignatureDate", 6), 'Europe/Paris'
        )
    )      as transporter_transport_signature_date,
    toNullable(
        toString("workerCompanyName")
    )      as worker_company_name,
    toNullable(
        toString("workerCompanySiret")
    )      as worker_company_siret,
    toNullable(
        toString("workerCompanyAddress")
    )      as worker_company_address,
    toNullable(
        toString("workerCompanyContact")
    )      as worker_company_contact,
    toNullable(
        toString("workerCompanyPhone")
    )      as worker_company_phone,
    toNullable(
        toString("workerCompanyMail")
    )      as worker_company_mail,
    toNullable(
        toBool("workerWorkHasEmitterPaperSignature")
    )      as worker_work_has_emitter_paper_signature,
    toNullable(
        toString("workerWorkSignatureAuthor")
    )      as worker_work_signature_author,
    toNullable(
        toTimezone(toDateTime64("workerWorkSignatureDate", 6), 'Europe/Paris')
    )      as worker_work_signature_date,
    toNullable(
        toString("brokerCompanyName")
    )      as broker_company_name,
    toNullable(
        toString("brokerCompanySiret")
    )      as broker_company_siret,
    toNullable(
        toString("brokerCompanyAddress")
    )      as broker_company_address,
    toNullable(
        toString("brokerCompanyContact")
    )      as broker_company_contact,
    toNullable(
        toString("brokerCompanyPhone")
    )      as broker_company_phone,
    toNullable(
        toString("brokerCompanyMail")
    )      as broker_company_mail,
    toNullable(
        toString("brokerRecepisseNumber")
    )      as broker_recepisse_number,
    toLowCardinality(
        toNullable(toString("brokerRecepisseDepartment"))
    )      as broker_recepisse_department,
    toNullable(
        toTimezone(
            toDateTime64("brokerRecepisseValidityLimit", 6), 'Europe/Paris'
        )
    )      as broker_recepisse_validity_limit,
    toNullable(
        toString("destinationOperationNextDestinationCompanyName")
    )      as destination_operation_next_destination_company_name,
    toNullable(
        toString("destinationOperationNextDestinationCompanySiret")
    )      as destination_operation_next_destination_company_siret,
    toNullable(
        toString("destinationOperationNextDestinationCompanyVatNumber")
    )      as destination_operation_next_destination_company_vat_number,
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
        toString("destinationOperationNextDestinationCap")
    )      as destination_operation_next_destination_cap,
    toNullable(
        replaceAll(
            toString("destinationOperationNextDestinationPlannedOperationCode"),
            ' ',
            ''
        )
    )      as destination_operation_next_destination_planned_operation_code,
    toNullable(
        toBool("weightIsEstimate")
    )      as weight_is_estimate,
    toNullable(
        toString("emitterCustomInfo")
    )      as emitter_custom_info,
    toNullable(
        toString("destinationCustomInfo")
    )      as destination_custom_info,
    toNullable(
        toString("ecoOrganismeName")
    )      as eco_organisme_name,
    toNullable(
        toString("ecoOrganismeSiret")
    )      as eco_organisme_siret,
    toNullable(
        toString("forwardingId")
    )      as forwarding_id,
    toNullable(
        toString("groupedInId")
    )      as grouped_in_id,
    toNullable(
        toBool("wastePop")
    )      as waste_pop,
    toNullable(
        toString("destinationOperationDescription")
    )      as destination_operation_description,
    toNullable(
        toBool("workerIsDisabled")
    )      as worker_is_disabled,
    toNullable(
        toBool("workerCertificationHasSubSectionFour")
    )      as worker_certification_has_sub_section_four,
    toNullable(
        toBool("workerCertificationHasSubSectionThree")
    )      as worker_certification_has_sub_section_three,
    toNullable(
        toString("workerCertificationCertificationNumber")
    )      as worker_certification_certification_number,
    toNullable(
        toTimezone(
            toDateTime64("workerCertificationValidityLimit", 6), 'Europe/Paris'
        )
    )      as worker_certification_validity_limit,
    toNullable(
        toString("workerCertificationOrganisation")
    )      as worker_certification_organisation,
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
    toLowCardinality(
        toNullable(toString("destinationOperationMode"))
    )      as destination_operation_mode,
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
    assumeNotNull(
        toInt256("rowNumber")
    )      as row_number,
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
    )      as can_access_draft_org_ids
from source
