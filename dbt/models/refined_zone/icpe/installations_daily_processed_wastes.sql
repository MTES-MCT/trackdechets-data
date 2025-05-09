{{
  config(
    materialized = 'table',
   tags =  ["fiche-etablissements"]
    )
}}
SELECT 
  siret,
  rubrique,
  raison_sociale,
  codes_aiot,
  quantite_autorisee,
  null as objectif_quantite_traitee,
  day_of_processing,
  quantite_traitee 
FROM {{ ref('installations_daily_processed_dangerous_wastes') }} dd
UNION ALL
SELECT 
  siret,
  rubrique,
  raison_sociale,
  codes_aiot,
  quantite_autorisee,
  objectif_quantite_traitee,
  day_of_processing,
  toDecimal256(quantite_traitee,30) as quantite_traitee
FROM {{ ref('installations_daily_processed_non_dangerous_wastes') }} dnd
