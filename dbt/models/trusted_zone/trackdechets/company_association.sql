{{
  config(
    materialized = 'table',
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'company_association') }}
)

SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toString("role")) as role,
    assumeNotNull(toString("companyId")) as company_id,
    assumeNotNull(toString("userId")) as user_id,
    toNullable(toDateTime64("createdAt", 6)) as created_at,
    assumeNotNull(toBool("automaticallyAccepted")) as automatically_accepted,
    assumeNotNull(toBool("notificationIsActiveBsdRefusal")) as notification_is_active_bsd_refusal,
    assumeNotNull(toBool("notificationIsActiveBsdaFinalDestinationUpdate")) as notification_is_active_bsda_final_destination_update,
    assumeNotNull(toBool("notificationIsActiveMembershipRequest")) as notification_is_active_membership_request,
    assumeNotNull(toBool("notificationIsActiveRevisionRequest")) as notification_is_active_revision_request,
    assumeNotNull(toBool("notificationIsActiveSignatureCodeRenewal")) as notification_is_active_signature_code_renewal
 FROM source
