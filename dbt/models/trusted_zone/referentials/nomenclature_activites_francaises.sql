select
    code_section,
    libelle_section,
    code_division,
    libelle_division,
    code_groupe,
    libelle_groupe,
    code_classe,
    libelle_classe,
    code_sous_classe,
    libelle_sous_classe
from
    {{ ref('stg_naf_2008') }}
