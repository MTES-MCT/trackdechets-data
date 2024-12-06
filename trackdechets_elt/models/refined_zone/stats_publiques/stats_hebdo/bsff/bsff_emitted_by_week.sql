{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bsff_counts("emitter_emission_signature_date", "emissions", "quantite_emise") }}
