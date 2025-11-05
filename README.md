# Plateforme Data du Projet Trackdéchets

Ce repository contient les différentes briques de la plateforme data du projet Trackdéchets. Notamment :

- [Airflow](https://airflow.apache.org/) (orchestration) ;
- [ClickHouse](https://clickhouse.com/) (base de données) ;
- [dbt](https://www.getdbt.com/) (transformation de données) ;
- [Metabase](https://www.metabase.com/) (BI/visualisation de données).

Chacune de ces briques est indépendante. Vous pouvez vous référez au README dans chacun des dossiers pour plus d'informations.

## Pré-requis

1. Docker et docker-compose
2. Python 3.12 et [uv](https://docs.astral.sh/uv/)

## Installation

1. Cloner le dépôt de code

```bash
git clone https://github.com/MTES-MCT/trackdechets-data
cd trackdechets-data
```

2. Installer les dépendances avec uv

```
uv sync --frozen
```

Python est nécessaire pour utiliser `dbt` et certains des scripts du dossier `scripts`.

## Intégration continue

Lorsqu'un commit est push dans `main`, une Github Action synchronise les changements avec l'instance de production qui héberge l'ensemble des services.
