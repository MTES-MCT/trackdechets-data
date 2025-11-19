select
    code_operation,
    rubrique
from {{ ref('stg_referentiel_codes_operation_rubriques') }}