{{
  config(
    materialized = 'table',
    )
}}

with source as (
    select *
    from {{ ref('registry_ssd') }}
    where is_latest and not is_cancelled
)

select
        id,
        created_at,
        updated_at,
        import_id,
        is_latest,
        is_cancelled,
        created_by_id,
        public_id,
        report_for_company_siret,
        report_for_company_name,
        report_for_company_address,
        report_for_company_city,
        report_for_company_postal_code,
        report_as_company_siret,
        weight_value,
        weight_is_estimate,
        volume,
        waste_code,
        waste_code_bale,
        waste_description,
        secondary_waste_codes,
        secondary_waste_descriptions,
        dispatch_date,
        use_date,
        processing_date,
        processing_end_date,
        operation_code,
        operation_mode,
        product,
        administrative_act_reference,
        destination_company_type,
        destination_company_org_id,
        destination_company_name,
        destination_company_address,
        destination_company_city,
        destination_company_postal_code,
        destination_company_country_code
    from source
