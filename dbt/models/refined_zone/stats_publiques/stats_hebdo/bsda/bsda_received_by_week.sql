{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsda","destination_reception_date", "receptions", "quantite_recue") }}
