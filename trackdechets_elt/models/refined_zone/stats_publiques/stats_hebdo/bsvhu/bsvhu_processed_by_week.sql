{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","destination_operation_date", "traitements", "quantite_traitee") }}
