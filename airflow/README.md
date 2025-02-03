# Airflow Trackdéchets

## Variables d'environnement

Pour fonctionner, Airflow a besoin d'un fichier `.env`.

1. Ajouter la variable `AIRFLOW_UID` :

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
