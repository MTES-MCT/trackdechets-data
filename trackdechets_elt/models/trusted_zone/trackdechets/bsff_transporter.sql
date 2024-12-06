with source as (
    select * from {{ source('trackdechets_production', 'bsff_transporter_raw') }}
),

renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("createdAt") }}                           as created_at,
        {{ adapter.quote("updatedAt") }}                           as updated_at,
        {{ adapter.quote("number") }},
        {{ adapter.quote("bsffId") }}                              as bsff_id,
        {{ adapter.quote("transporterCompanySiret") }}             as transporter_company_siret,
        {{ adapter.quote("transporterCompanyName") }}              as transporter_company_name,
        {{ adapter.quote("transporterCompanyVatNumber") }}         as transporter_company_vat_number,
        {{ adapter.quote("transporterCompanyAddress") }}           as transporter_company_address,
        {{ adapter.quote("transporterCompanyContact") }}           as transporter_company_contact,
        {{ adapter.quote("transporterCompanyPhone") }}             as transporter_company_phone,
        {{ adapter.quote("transporterCompanyMail") }}              as transporter_company_mail,
        {{ adapter.quote("transporterCustomInfo") }}               as transporter_custom_info,
        {{ adapter.quote("transporterRecepisseIsExempted") }}      as transporter_recepisse_is_exempted,
        {{ adapter.quote("transporterRecepisseNumber") }}          as transporter_recepisse_number,
        {{ adapter.quote("transporterRecepisseDepartment") }}      as transporter_recepisse_department,
        {{ adapter.quote("transporterRecepisseValidityLimit") }}   as transporter_recepisse_validity_limit,
        {{ adapter.quote("transporterTransportMode") }}            as transporter_transport_mode,
        {{ adapter.quote("transporterTransportPlates") }}          as transporter_transport_plates,
        cast(
            {{ adapter.quote("transporterTransportTakenOverAt") }} as timestamptz
        )                                     as transporter_transport_taken_over_at,
        {{ adapter.quote("transporterTransportSignatureAuthor") }} as transporter_transport_signature_author,
        {{ adapter.quote("transporterTransportSignatureDate") }}   as transporter_tranport_signature_date

    from source
)

select * from renamed
