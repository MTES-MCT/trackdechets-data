{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","transporter_transport_taken_over_at", "envois", "quantite_envoyee") }}
