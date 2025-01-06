{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsdd","processed_at", "traitements","quantite_traitee",false) }}
