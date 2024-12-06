{{
  config(
    materialized = 'table',
    indexes = [{
        "columns" : ["siret","email_admin"],
        "unique":true
    }]
    )
}}

with companies as (
    select distinct recipient_company_siret as siret
    from
        {{ ref('bsdd') }}
    where
        quantity_received = 0
        and received_at is not null
        and created_at >= '2023-01-01'
        and not is_deleted
        and waste_acceptation_status = 'ACCEPTED'
        and status in (
            'RECEIVED',
            'FOLLOWED_WITH_PNTTD',
            'PROCESSED',
            'AWAITING_GROUP',
            'NO_TRACEABILITY',
            'GROUPED'
        )
)

select
    c.siret,
    ca.company_name as nom_etablissement,
    ca.user_email   as email_admin,
    ca.user_name    as nom_admin
from companies as c
inner join {{ ref('companies_admins') }} as ca on c.siret = ca.company_siret
