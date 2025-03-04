# Trackdéchets ELT

## Migration données :

    1. Données de prod Trackdéchets
        a. Connecter le schéma default$default de la db de prod avec un Postgres Database Engine
        b. Créer les vues data eng sur la table de prod avec le script `script/create_data_eng_views_prod_db.sql`
        c. Lancer le dossier trusted_zone_trackdechets avec dbt

    2. Migration données RNDTS
        a. Connecter le schéma trusted_zone_rndts de la db de prod avec un Postgres Database Engine
        b. Créer les tables dans clickhouse avec le script `script/create_data_eng_views_prod_db.sql`
