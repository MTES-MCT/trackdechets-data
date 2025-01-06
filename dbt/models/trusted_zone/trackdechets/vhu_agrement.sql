{{
  config(
    materialized = 'table',
    indexes = [
        { "columns": ["id"], "unique": True}
    ]
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'vhu_agrement') }}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toString("agrementNumber")) as agrement_number,
    toLowCardinality(assumeNotNull(toString("department"))) as department
 FROM source

