{{
  config(
    materialized = 'table',
   indexes=[{"columns":["siret","day_of_processing"]}]
    )
}}
SELECT
    siret,
    date_reception AS day_of_processing,
    rubrique,
    quantite       AS quantite_traitee
FROM {{ ref('daily_processed_waste_by_rubrique_non_dangerous_waste') }}
UNION ALL
SELECT
    siret,
    day_of_processing,
    rubrique,
    quantite_traitee
FROM {{ ref('daily_processed_waste_by_rubrique_dangerous_waste') }}
