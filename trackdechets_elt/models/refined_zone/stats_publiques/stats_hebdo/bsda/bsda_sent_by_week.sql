{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsda","transporter_transport_signature_date", "envois", "quantite_envoyee") }}
