{{
  config(
    materialized = 'table',
    )
}}

SELECT
    assumeNotNull(toString("id")) as id,
    assumeNotNull(toBool("hasSubSectionFour")) as has_sub_section_four,
    assumeNotNull(toBool("hasSubSectionThree")) as has_sub_section_three,
    toNullable(toString("certificationNumber")) as certification_number,
    toNullable(toDateTime64("validityLimit", 6, 'Europe/Paris') - timeZoneOffset(toTimeZone("validityLimit",'Europe/Paris'))) as validity_limit,
    toLowCardinality(toNullable(toString("organisation"))) as organisation
from {{ source('trackdechets_production', 'worker_certification') }}