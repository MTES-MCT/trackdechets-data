# dbt Trackdéchets

Ceci est le projet [dbt](https://www.getdbt.com/) de Trackdéchets. Ce projet est responsable de plusieurs aspects :

- Gérer l'intégration des données venant de bases PostgreSQL externes (comme celle de production de Trackdéchets)
- Matérialiser toutes les transformations de données faites à partir des données de la `raw_zone` de l'entrepôt de données.

## Installation des dépendances

Pour utiliser ce projet, il est nécessaire d'installer les dépendances à l'aide de [uv](https://github.com/astral-sh/uv) et du fichier `uv.lock` présent à la racine du dépot :

```bash
uv sync --frozen
```

## Configuration du profil

Ce projet dbt nécessite plusieurs variables d'envionnement pour fonctionner :

```
# Configuration générale
DBT_PROJECT_DIR=/trackdechets-data/dbt # Chemin vers ce projet dbt
DBT_PROFILES_DIR=/trackdechets-data/dbt/profiles # Chemin vers le dossier des profils du projet dbt

#  Configuration de la connection à la base pour la target dev
DBT_HOST=localhost
DBT_USER=user
DBT_PASSWORD=pa$$word
DBT_PORT=8123

#  Configuration de la connection à la base pour la target prod
DBT_HOST_PROD=localhost
DBT_USER_PROD=user_prod
DBT_PASSWORD_PROD=pa$$wordprod
DBT_PORT_PROD=8123
```

Il est conseillé de stocker ces valeurs dans un fichier `.env`.

## Utilisation du projet dbt

Une fois les dépendances installées et le profil configuré, vous pouvez exécuter les modèles dbt avec la commande suivante :

> [!NOTE]
> Par défaut, dbt lance ces commandes sur la target `dev` qui correspond à une base ClickHouse locale. Pour lancer directement sur la base de prod, il est possible d'utiliser le paramètre dbt `--target prod`.

```bash
uv run --env-file .env dbt run
```

Cette commande exécutera tous les modèles définis dans le projet. Vous pouvez également exécuter des modèles spécifiques en utilisant la commande suivante :

```bash
uv run --env-file .env dbt run <nom_du_modèle>
```

Pour tester les modèles, vous pouvez utiliser la commande suivante :

```bash
uv run --env-file .env dbt test
```

Cela exécutera tous les tests définis dans le projet. Vous pouvez également exécuter des tests spécifiques en utilisant la commande suivante :

```bash
uv run --env-file .env dbt test <nom_du_modèle>
```

Pour générer la documentation du projet, vous pouvez utiliser la commande suivante :

```bash
uv run --env-file .env dbt docs generate
```

Et pour servir cette documentation localement:

```bash
uv run --env-file .env dbt docs serve
```

## Organisation du projet dbt

Le différents modèles dans le dossier `models` sont organisés en deux zones : `trusted_zone`et `refined_zone` ayant chacune leur sous-dossier attitré.
À ceux-ci s'ajoutent un dossier `sources` qui contient les sources de données correspondant aux données dans la `raw_zone`.

## Création automatique des fichiers YAML

Chaque modèle est accompagné d'un fichier YAML qui le documente, décrit et dans lequel certaines options peuvent être configurées.
Ces fichiers peuvent être générés à partir des modèles eux-mêmes (à condition qu'ils aient été déjà matérialisés en base) à l'aide de l'outil [dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis) avec la commande suivante :

```bash
uv run --env-file .env dbt-osmosis yaml refactor
```

## Formatage

[sqlfluff](https://github.com/sqlfluff/sqlfluff) est utilisé comme formateur pour le projet. Les règles personnalisées choisies sont dans le fichier `.sqlfluff`.

Pour formater le projet, il faut utiliser la commande suivante :

```bash
uv run --env-file .env sqlfluff fix .
```

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
