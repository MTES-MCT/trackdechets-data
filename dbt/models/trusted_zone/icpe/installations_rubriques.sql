select
    raison_sociale,
    siret,
    code_aiot,
    code_etat_site,
    libelle_etat_site,
    regime,
    quantite_projet,
    capacite_projet,
    capacite_totale,
    unite,
    etat_technique_rubrique,
    etat_administratif_rubrique,
    quantite_totale,
    rubrique
from {{ ref('installations_rubriques_2026') }}

