{{
  config(
    materialized = 'table',
   indexes=[{"columns":["siret","day_of_processing","rubrique"], "unique":True}],
   tags =  ["fiche-etablissements"]
    )
}}
SELECT *
FROM {{ ref('installations_daily_processed_dangerous_wastes') }}
UNION ALL
SELECT *
FROM {{ ref('installations_daily_processed_non_dangerous_wastes') }}
