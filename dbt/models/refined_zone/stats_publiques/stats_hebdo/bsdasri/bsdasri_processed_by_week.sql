{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsdasri","destination_operation_date", "traitements", "quantite_traitee") }}
