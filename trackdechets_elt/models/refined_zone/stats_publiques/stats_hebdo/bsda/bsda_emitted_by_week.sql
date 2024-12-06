{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsda","emitter_emission_signature_date", "emissions", "quantite_emise") }}
