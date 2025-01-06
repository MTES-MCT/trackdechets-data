{{
  config(
    materialized = 'table',
    indexes = [{"columns":["numero_gistrid"],"unique":true},{"columns":["siret"],"unique":false}]
    )
}}

select * from {{ ref('installations_gistrid') }}
union all
select * from {{ ref('notifiants') }}
