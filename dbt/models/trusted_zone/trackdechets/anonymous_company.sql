{{
  config(
    materialized = 'table'
    )
}}
-- noqa: disable=AL02
with source as (
    select * from {{ source('trackdechets_production', 'anonymous_company') }}
)

select
    assumeNotNull(toString("id"))                                  as id,
    toNullable(toString("siret"))                                  as siret,
    assumeNotNull(toString("name"))                                as name,
    assumeNotNull(toString("address"))                             as address,
    toLowCardinality(assumeNotNull(toString("codeNaf")))           as code_naf,
    toLowCardinality(assumeNotNull(toString("libelleNaf")))
        as libelle_naf,
    toLowCardinality(assumeNotNull(toString("codeCommune")))
        as code_commune,
    toNullable(toString("vatNumber"))
        as vat_number,
    assumeNotNull(toString("orgId"))                               as org_id,
    toLowCardinality(assumeNotNull(toString("etatAdministratif")))
        as etat_administratif
from source
