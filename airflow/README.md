# Airflow Trackdéchets

## Variables d'environnement

Pour fonctionner, Airflow a besoin d'un fichier `.env`.

1. Ajouter la variable `AIRFLOW_UID` :

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Créer le réseau qui permettra à Clickhouse et Airflow de communiquer :

```bash
docker network create data_external_network
```
