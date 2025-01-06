{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","created_at", "creations", "quantite_tracee") }}
