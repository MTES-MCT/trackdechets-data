{{
  config(
    materialized = 'table',
    )
}}

SELECT
    assumeNotNull(toString("id"))
        AS id,
    assumeNotNull(toBool("hasSubSectionFour"))
        AS has_sub_section_four,
    assumeNotNull(toBool("hasSubSectionThree"))
        AS has_sub_section_three,
    toNullable(toString("certificationNumber"))
        AS certification_number,
    toNullable(toTimezone(toDateTime64("validityLimit", 6), 'Europe/Paris'))
        AS validity_limit,
    toLowCardinality(toNullable(toString("organisation")))
        AS organisation
FROM {{ source('trackdechets_production', 'worker_certification') }}
