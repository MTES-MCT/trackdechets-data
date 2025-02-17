{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
    on_schema_change='append_new_columns'
    )
}}

with source as (
    select * from {{ source('trackdechets_production', 'bsda') }} as b
    {% if is_incremental() %}
        where
            b.updatedat
            >= (select toString(toStartOfDay(max(updated_at))) from {{ this }})
    {% endif %}
)

select
    assumeNotNull(
        toString(id)
    )      as id,
    assumeNotNull(
        toDateTime64(createdat, 6, 'Europe/Paris')
        - timeZoneOffset(toTimeZone(createdat, 'Europe/Paris'))
    )      as created_at,
    assumeNotNull(
        toDateTime64(updatedat, 6, 'Europe/Paris')
        - timeZoneOffset(toTimeZone(updatedat, 'Europe/Paris'))
    )      as updated_at,
    assumeNotNull(
        toBool(isdraft)
    )      as is_draft,
    assumeNotNull(
        toBool(isdeleted)
    )      as is_deleted,
    toLowCardinality(
        assumeNotNull(toString(status))
    )      as status,
    assumeNotNull(
        toString(type)
    )      as type,
    toNullable(
        toBool(emitterisprivateindividual)
    )      as emitter_is_private_individual,
    toNullable(
        toString(emittercompanyname)
    )      as emitter_company_name,
    toNullable(
        toString(emittercompanysiret)
    )      as emitter_company_siret,
    toNullable(
        toString(emittercompanyaddress)
    )      as emitter_company_address,
    toNullable(
        toString(emittercompanycontact)
    )      as emitter_company_contact,
    toNullable(
        toString(emittercompanyphone)
    )      as emitter_company_phone,
    toNullable(
        toString(emittercompanymail)
    )      as emitter_company_mail,
    toNullable(
        toString(emitterpickupsitename)
    )      as emitter_pickup_site_name,
    toNullable(
        toString(emitterpickupsiteaddress)
    )      as emitter_pickup_site_address,
    toNullable(
        toString(emitterpickupsitecity)
    )      as emitter_pickup_site_city,
    toLowCardinality(
        toNullable(toString(emitterpickupsitepostalcode))
    )      as emitter_pickup_site_postal_code,
    toNullable(
        toString(emitterpickupsiteinfos)
    )      as emitter_pickup_site_infos,
    toNullable(
        toString(emitteremissionsignatureauthor)
    )      as emitter_emission_signature_author,
    toNullable(
        toDateTime64(emitteremissionsignaturedate, 6, 'Europe/Paris')
        - timeZoneOffset(
            toTimeZone(emitteremissionsignaturedate, 'Europe/Paris')
        )
    )      as emitter_emission_signature_date,
    toLowCardinality(
        toNullable(toString(wastecode))
    )      as waste_code,
    toNullable(
        toString(wastefamilycode)
    )      as waste_family_code,
    toNullable(
        toString(wastematerialname)
    )      as waste_material_name,
    toNullable(
        toString(wasteconsistence)
    )      as waste_consistence,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString(wastesealnumbers), 2, length(wastesealnumbers) - 2
                ),
                ''
            )
        )
    )      as waste_seal_numbers,
    toNullable(
        toString(wasteadr)
    )      as waste_adr,
    assumeNotNull(
        toString(packagings)
    )      as packagings,
    toNullable(toDecimal256(weightvalue, 30))
    / 1000 as weight_value,
    toNullable(
        toString(destinationcompanyname)
    )      as destination_company_name,
    toNullable(
        toString(destinationcompanysiret)
    )      as destination_company_siret,
    toNullable(
        toString(destinationcompanyaddress)
    )      as destination_company_address,
    toNullable(
        toString(destinationcompanycontact)
    )      as destination_company_contact,
    toNullable(
        toString(destinationcompanyphone)
    )      as destination_company_phone,
    toNullable(
        toString(destinationcompanymail)
    )      as destination_company_mail,
    toNullable(
        toString(destinationcap)
    )      as destination_cap,
    toNullable(
        replaceAll(toString(destinationplannedoperationcode), ' ', '')
    )      as destination_planned_operation_code,
    toNullable(
        toDateTime64(destinationreceptiondate, 6, 'Europe/Paris')
        - timeZoneOffset(toTimeZone(destinationreceptiondate, 'Europe/Paris'))
    )      as destination_reception_date,
    toNullable(toDecimal256(destinationreceptionweight, 30))
    / 1000 as destination_reception_weight,
    toNullable(
        toString(destinationreceptionacceptationstatus)
    )      as destination_reception_acceptation_status,
    toNullable(
        toString(destinationreceptionrefusalreason)
    )      as destination_reception_refusal_reason,
    toLowCardinality(
        toNullable(replaceAll(toString(destinationoperationcode), ' ', ''))
    )      as destination_operation_code,
    toNullable(
        toDateTime64(destinationoperationdate, 6, 'Europe/Paris')
        - timeZoneOffset(toTimeZone(destinationoperationdate, 'Europe/Paris'))
    )      as destination_operation_date,
    toNullable(
        toString(destinationoperationsignatureauthor)
    )      as destination_operation_signature_author,
    toNullable(
        toDateTime64(destinationoperationsignaturedate, 6, 'Europe/Paris')
        - timeZoneOffset(
            toTimeZone(destinationoperationsignaturedate, 'Europe/Paris')
        )
    )      as destination_operation_signature_date,
    toNullable(
        toDateTime64(transportertransportsignaturedate, 6, 'Europe/Paris')
        - timeZoneOffset(
            toTimeZone(transportertransportsignaturedate, 'Europe/Paris')
        )
    )      as transporter_transport_signature_date,
    toNullable(
        toString(workercompanyname)
    )      as worker_company_name,
    toNullable(
        toString(workercompanysiret)
    )      as worker_company_siret,
    toNullable(
        toString(workercompanyaddress)
    )      as worker_company_address,
    toNullable(
        toString(workercompanycontact)
    )      as worker_company_contact,
    toNullable(
        toString(workercompanyphone)
    )      as worker_company_phone,
    toNullable(
        toString(workercompanymail)
    )      as worker_company_mail,
    toNullable(
        toBool(workerworkhasemitterpapersignature)
    )      as worker_work_has_emitter_paper_signature,
    toNullable(
        toString(workerworksignatureauthor)
    )      as worker_work_signature_author,
    toNullable(
        toDateTime64(workerworksignaturedate, 6, 'Europe/Paris')
        - timeZoneOffset(toTimeZone(workerworksignaturedate, 'Europe/Paris'))
    )      as worker_work_signature_date,
    toNullable(
        toString(brokercompanyname)
    )      as broker_company_name,
    toNullable(
        toString(brokercompanysiret)
    )      as broker_company_siret,
    toNullable(
        toString(brokercompanyaddress)
    )      as broker_company_address,
    toNullable(
        toString(brokercompanycontact)
    )      as broker_company_contact,
    toNullable(
        toString(brokercompanyphone)
    )      as broker_company_phone,
    toNullable(
        toString(brokercompanymail)
    )      as broker_company_mail,
    toNullable(
        toString(brokerrecepissenumber)
    )      as broker_recepisse_number,
    toLowCardinality(
        toNullable(toString(brokerrecepissedepartment))
    )      as broker_recepisse_department,
    toNullable(
        toDateTime64(brokerrecepissevaliditylimit, 6, 'Europe/Paris')
        - timeZoneOffset(
            toTimeZone(brokerrecepissevaliditylimit, 'Europe/Paris')
        )
    )      as broker_recepisse_validity_limit,
    toNullable(
        toString(destinationoperationnextdestinationcompanyname)
    )      as destination_operation_next_destination_company_name,
    toNullable(
        toString(destinationoperationnextdestinationcompanysiret)
    )      as destination_operation_next_destination_company_siret,
    toNullable(
        toString(destinationoperationnextdestinationcompanyvatnumber)
    )      as destination_operation_next_destination_company_vat_number,
    toNullable(
        toString(destinationoperationnextdestinationcompanyaddress)
    )      as destination_operation_next_destination_company_address,
    toNullable(
        toString(destinationoperationnextdestinationcompanycontact)
    )      as destination_operation_next_destination_company_contact,
    toNullable(
        toString(destinationoperationnextdestinationcompanyphone)
    )      as destination_operation_next_destination_company_phone,
    toNullable(
        toString(destinationoperationnextdestinationcompanymail)
    )      as destination_operation_next_destination_company_mail,
    toNullable(
        toString(destinationoperationnextdestinationcap)
    )      as destination_operation_next_destination_cap,
    toNullable(
        replaceAll(
            toString(destinationoperationnextdestinationplannedoperationcode),
            ' ',
            ''
        )
    )      as destination_operation_next_destination_planned_operation_code,
    toNullable(
        toBool(weightisestimate)
    )      as weight_is_estimate,
    toNullable(
        toString(emittercustominfo)
    )      as emitter_custom_info,
    toNullable(
        toString(destinationcustominfo)
    )      as destination_custom_info,
    toNullable(
        toString(ecoorganismename)
    )      as eco_organisme_name,
    toNullable(
        toString(ecoorganismesiret)
    )      as eco_organisme_siret,
    toNullable(
        toString(forwardingid)
    )      as forwarding_id,
    toNullable(
        toString(groupedinid)
    )      as grouped_in_id,
    toNullable(
        toBool(wastepop)
    )      as waste_pop,
    toNullable(
        toString(destinationoperationdescription)
    )      as destination_operation_description,
    toNullable(
        toBool(workerisdisabled)
    )      as worker_is_disabled,
    toNullable(
        toBool(workercertificationhassubsectionfour)
    )      as worker_certification_has_sub_section_four,
    toNullable(
        toBool(workercertificationhassubsectionthree)
    )      as worker_certification_has_sub_section_three,
    toNullable(
        toString(workercertificationcertificationnumber)
    )      as worker_certification_certification_number,
    toNullable(
        toDateTime64(workercertificationvaliditylimit, 6, 'Europe/Paris')
        - timeZoneOffset(
            toTimeZone(workercertificationvaliditylimit, 'Europe/Paris')
        )
    )      as worker_certification_validity_limit,
    toNullable(
        toString(workercertificationorganisation)
    )      as worker_certification_organisation,
    assumeNotNull(
        splitByChar(
            ',',
            cOALESCE(
                substring(
                    toString(intermediariesorgids),
                    2,
                    length(intermediariesorgids) - 2
                ),
                ''
            )
        )
    )      as intermediaries_org_ids,
    toLowCardinality(
        toNullable(toString(destinationoperationmode))
    )      as destination_operation_mode,
    assumeNotNull(
        splitByChar(
            ',',
            coalesce(
                substring(
                    toString(transportersorgids),
                    2,
                    length(transportersorgids) - 2
                ),
                ''
            )
        )
    )      as transporters_org_ids,
    assumeNotNull(
        toInt256(rownumber)
    )      as row_number,
    assumeNotNull(
        splitByChar(
            ',',
            coalesce(
                substring(
                    toString(canaccessdraftorgids),
                    2,
                    length(canaccessdraftorgids) - 2
                ),
                ''
            )
        )
    )      as can_access_draft_org_ids
from source
