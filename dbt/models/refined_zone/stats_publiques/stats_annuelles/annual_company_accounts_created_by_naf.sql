{{
  config(
    materialized = 'table',
    )}}

select
    toYear(ce.created_at)  as annee,
    toNullable(ce.code_sous_classe) as ce.code_sous_classe,
    toNullable(max(code_section))        as code_section,
    toNullable(max(libelle_section))     as libelle_section,
    toNullable(max(code_division))       as code_division,
    toNullable(max(libelle_division))    as libelle_division,
    toNullable(max(code_groupe))         as code_groupe,
    toNullable(max(libelle_groupe))      as libelle_groupe,
    toNullable(max(code_classe))         as code_classe,
    toNullable(max(libelle_classe))      as libelle_classe,
    toNullable(max(libelle_sous_classe)) as libelle_sous_classe,
    count(*)                 as nombre_etablissements
from
    {{ ref('company_enriched') }} as ce
where
    ce.created_at >= '2020-01-01'
    and ce.created_at < toStartOfWeek(now('Europe/Paris'),1,'Europe/Paris')
group by 1,2
