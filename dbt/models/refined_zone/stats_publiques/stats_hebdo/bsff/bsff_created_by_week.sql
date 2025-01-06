{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bsff_counts("created_at", "creations", "quantite_tracee") }}
