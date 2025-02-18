{{
  config(
    materialized = 'table',
    )}}

select * from {{ ref('installations_gistrid') }}
union all
select * from {{ ref('notifiants') }}
