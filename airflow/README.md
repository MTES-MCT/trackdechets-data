# Airflow Trackdéchets

## Installation

Pour fonctionner, Airflow a besoin d'un fichier `.env`.

1. Ajouter la variable `AIRFLOW_UID` :

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Ajouter dans le fichier `.env` les variables d'environnement nécessaires en utilisant l'exemple dans le fichier `.env.dist`

3. Créer le réseau qui permettra à Clickhouse et Airflow de communiquer :

```bash
docker network create data_external_network
```

4. Lancer les containers :

```bash
docker compose up
```

## Dépendances Python

L'image Airflow utilisée installe les dépendances présente dans le fichier `requirements.txt`. Pour que Airflow puisse utiliser de nouvelles dépendances Python, il faut ajouter les bibliothèques Python voulues dans ce fichier. En cas de modification de ces dépendances, il faut faire un nouveau build docker : `docker compose up --build`.

## Connections Airflow

Les DAGs contenus dans ce projet nécessitent une connection de type `generic` nommée `td_datawarehouse`. Elle permet de se connecter à la base ClickHouse.

## DAGs

Les DAGs sont conteus dans le dossier `dags` et organisés en sous-dossiers.

### `dbt`

Ce dossier contient le générateur de DAGs qui matèrialise le projet dbt en DAG Airflow grâce à [Cosmos](https://github.com/astronomer/astronomer-cosmos). Le projet `dbt` est monté comme un volume dans le container Airflow dans le dossier `/opt/airflow/dbt`.

### `dlt_pipelines`

Ce dossier contient les DAGs d'intégration de données via [dlt](https://dlthub.com/).
Actuellement `dlt` est utilisé uniqument pour l'intégration dans l'entrepôt de données des données Zammad.

### `etl_insee`

Deux DAGs qui permettent l'intégration de la base SIRENE des établissement ( stock établissement) ainsi que les référentiels géographique de l'INSEE.

### `geocoding`

Ce DAG est utilisé afin de géocoder les établissements présent dans les données Trackdéchets pour lesquels nous n'avons pas de localisation.

### `open_data`

Les deux DAGs de ce dossier sont ceux qui permettent la publication des données open-data sur `data.gouv.fr`.

### `trackdechets_search_sirene`

Contient les DAGs qui indexent les données de la base SIRENE (stock-etablissement et unite-legale) dans l'Elastic Search Trackdechets de production dédiée à ces deux bases.

### `utils`

Contient les modules python commun à chaques DAGs.
