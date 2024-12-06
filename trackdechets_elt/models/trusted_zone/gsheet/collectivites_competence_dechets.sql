select
    siren_epci  as siren,
    nom_complet as nom,
    dep_epci    as code_departement,
    nj_epci2023,
    fisc_epci2023,
    nb_com_2023,
    ptot_epci_2023,
    pmun_epci_2023
from
    {{ source('raw_zone_gsheet', 'collectivites_compentence_dechets') }}
