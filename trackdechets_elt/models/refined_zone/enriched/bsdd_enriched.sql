{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change='append_new_columns',
    indexes = [ 
        {'columns': ['id'], 'unique': True },
        {'columns': ['created_at'] },
        {'columns': ['updated_at'] },
        {'columns': ['received_at'] },
        {'columns': ['sent_at'] },
        {'columns': ['processed_at'] },
        { 'columns': ['emitter_company_siret'] },
        { 'columns' :['recipient_company_siret'] },
        { 'columns' :['current_transporter_org_id'] },
        { 'columns' :['waste_details_code'] },
        { 'columns' :['emitter_commune'] },
        { 'columns' :['emitter_departement'] },
        { 'columns' :['emitter_region'] },
        { 'columns' :['recipient_commune'] },
        { 'columns' :['recipient_departement'] },
        { 'columns' :['recipient_region'] }
    ]
) }}

{{ create_bordereaux_enriched_query('bsdd',True) }}
