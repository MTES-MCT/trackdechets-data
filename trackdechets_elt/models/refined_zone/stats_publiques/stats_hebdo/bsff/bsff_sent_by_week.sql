{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bsff_counts("transporter_transport_taken_over_at", "envois", "quantite_envoyee") }}
