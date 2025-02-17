{{
  config(
    materialized = 'table',
    indexes = [ 
        {'columns': ['id'] , 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['destination_company_siret'] },
        { 'columns' :['transporter_company_siret'], },
        { 'columns' :['waste_code'] },
    ]
    )
}}

with bsff_transporter_with_bsff as (
    select
        bt.id,
        bt.created_at,
        bt.updated_at,
        bt.number,
        bt.bsff_id,
        bt.transporter_company_siret,
        bt.transporter_company_name,
        bt.transporter_company_vat_number,
        bt.transporter_company_address,
        bt.transporter_company_contact,
        bt.transporter_company_phone,
        bt.transporter_company_mail,
        bt.transporter_custom_info,
        bt.transporter_recepisse_is_exempted,
        bt.transporter_recepisse_number,
        bt.transporter_recepisse_department,
        bt.transporter_recepisse_validity_limit,
        bt.transporter_transport_mode,
        bt.transporter_transport_plates,
        bt.transporter_transport_taken_over_at,
        bt.transporter_transport_signature_author,
        bt.transporter_tranport_signature_date,
        b.created_at as bordereau_created_at,
        b.emitter_company_siret,
        b.destination_company_siret,
        b.waste_code
    from {{ ref('bsff_transporter') }} as bt
    left join {{ ref('bsff') }} as b on bt.bsff_id = b.id
)

-- On va chercher le poids dans la table des contenants
select
    b.id,
    max(b.created_at)                 as created_at,
    max(b.updated_at)                 as updated_at,
    max(b.number)                     as number,
    max(b.bsff_id)                    as bsff_id,
    max(b.transporter_company_siret)  as transporter_company_siret,
    max(b.transporter_company_name)   as transporter_company_name,
    max(
        b.transporter_company_vat_number
    )                                 as transporter_company_vat_number,
    max(
        b.transporter_company_address
    )                                 as transporter_company_address,
    max(
        b.transporter_company_contact
    )                                 as transporter_company_contact,
    max(b.transporter_company_phone)  as transporter_company_phone,
    max(b.transporter_company_mail)   as transporter_company_mail,
    max(b.transporter_custom_info)    as transporter_custom_info,
    bool_and(
        b.transporter_recepisse_is_exempted
    )                                 as transporter_recepisse_is_exempted,
    max(
        b.transporter_recepisse_number
    )                                 as transporter_recepisse_number,
    max(
        b.transporter_recepisse_department
    )                                 as transporter_recepisse_department,
    max(
        b.transporter_recepisse_validity_limit
    )                                 as transporter_recepisse_validity_limit,
    max(b.transporter_transport_mode) as transporter_transport_mode,
    max(
        b.transporter_transport_plates
    )                                 as transporter_transport_plates,
    max(
        b.transporter_transport_taken_over_at
    )                                 as transporter_transport_taken_over_at,
    max(
        b.transporter_transport_signature_author
    )                                 as transporter_transport_signature_author,
    max(
        b.transporter_tranport_signature_date
    )                                 as transporter_tranport_signature_date,
    max(b.bordereau_created_at)       as bordereau_created_at,
    max(b.emitter_company_siret)      as emitter_company_siret,
    max(b.destination_company_siret)  as destination_company_siret,
    max(b.waste_code)                 as waste_code,
    sum(bp.acceptation_weight)        as acceptation_weight,
    max(bp.operation_code)            as operation_code
from bsff_transporter_with_bsff as b
left join {{ ref('bsff_packaging') }} as bp on b.bsff_id = bp.bsff_id
group by 1
