{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bsff_counts("destination_reception_date", "receptions", "quantite_recue") }}
