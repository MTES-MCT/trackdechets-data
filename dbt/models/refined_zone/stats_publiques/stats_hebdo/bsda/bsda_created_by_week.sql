{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsda","created_at", "creations", "quantite_tracee") }}
