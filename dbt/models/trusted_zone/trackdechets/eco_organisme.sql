{{
  config(
    materialized = 'table'
    )
}}

with source as (
    select *
    from {{ source('trackdechets_production', 'eco_organisme') }}
)
SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toString("siret")) as siret,
    assumeNotNull(toString("name")) as name,
    assumeNotNull(toString("address")) as address,
    assumeNotNull(toBool("handleBsdasri")) as handle_bsdasri,
    assumeNotNull(toBool("handleBsda")) as handle_bsda,
    assumeNotNull(toBool("handleBsvhu")) as handle_bsvhu
 FROM source