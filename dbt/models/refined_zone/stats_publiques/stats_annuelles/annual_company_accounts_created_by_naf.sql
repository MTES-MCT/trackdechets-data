{{
  config(
    materialized = 'table',
    )}}

select
    toYear(ce.created_at)  as annee,
    ce.code_sous_classe,
    max(code_section)        as code_section,
    max(libelle_section)     as libelle_section,
    max(code_division)       as code_division,
    max(libelle_division)    as libelle_division,
    max(code_groupe)         as code_groupe,
    max(libelle_groupe)      as libelle_groupe,
    max(code_classe)         as code_classe,
    max(libelle_classe)      as libelle_classe,
    max(libelle_sous_classe) as libelle_sous_classe,
    count(*)                 as nombre_etablissements
from
    {{ ref('company_enriched') }} as ce
where
    ce.created_at >= '2020-01-01'
    and ce.created_at < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
group by 1,2
