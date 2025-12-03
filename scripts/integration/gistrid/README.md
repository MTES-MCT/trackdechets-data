# Script d'intégration des données GISTRID dans ClickHouse

Ce script, `scripts/integration/gistrid/load_gistrid_data_ch.py`, importe les jeux de données GISTRID (notifications, notifiants, installations) provenant d’un fichier Excel ou CSV vers un entrepôt de données ClickHouse.  
Il est conçu pour être exécuté en tant qu’outil autonome en ligne de commande.

---

## 📦 Dépendances

Toutes les bibliothèques requises figurent dans le `pyproject.toml` du projet.  
Installez‑les avec **uv** (le gestionnaire de paquets recommandé pour ce dépôt) :

```bash
uv sync
```

Les packages essentiels sont :

- `clickhouse-connect`
- `polars`
- `argparse` (bibliothèque standard)
- `pathlib`, `tempfile`, `shutil`, `logging` (bibliothèques standard)

## 🚀 Utilisation

```bash
python scripts/integration/gistrid/load_gistrid_data_ch.py \
    [options] <file_type> <filepath>
```

### Arguments positionnels

| Argument      | Description                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------- |
| `<file_type>` | Type de jeu de données : `notifications`, `notifiants` ou `installations`.                                          |
| `<filepath>`  | Chemin vers le fichier source Excel (`.xlsx`) pour les notifications ou CSV (séparé par `;`) pour les autres types. |

### Options

| Option            | Valeur par défaut | Description                                                                                                         |
| ----------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------- |
| `--dwh_host`      | `localhost`       | Adresse du serveur ClickHouse.                                                                                      |
| `--dwh_http_port` | `8123`            | Port HTTP du serveur ClickHouse.                                                                                    |
| `--dwh_username`  | `default`         | Nom d’utilisateur pour l’authentification.                                                                          |
| `--dwh_password`  | `""`              | Mot de passe pour l’authentification.                                                                               |
| `--year`          | `None`            | Année/millesime du fichier qui sera ajoutée dans une colonne de la table (utile uniquement pour les notifications). |
| `--full-refresh`  | `False`           | Si utilisé, la table cible est supprimée avant d’y insérer les nouvelles données.                                   |

### Exemple

```bash
uv run scripts/integration/gistrid/load_gistrid_data_ch.py \
    --dwh_host 192.168.1.10 \
    --dwh_http_port 8123 \
    --dwh_username admin \
    --dwh_password secret \
    --year 2024 \
    --full-refresh \
    notifications \
    /data/gistrid/notifications_2024.xlsx
```

## 📊 Ce que fait le script

1. **Valide** `file_type`.
2. **Établit** une connexion à ClickHouse avec les identifiants fournis.
3. Optionnellement, **supprime** la table existante (`--full-refresh`).
4. **Crée** la table cible si elle n’existe pas déjà (DDL défini dans `schemas/tables_ddl.py`).
5. Lit le fichier source dans un DataFrame Polars, en convertissant les colonnes vers des chaînes de caractères selon le besoin.
6. Pour les notifications, ajoute une colonne `annee` contenant l’année fournie.
7. Écrit le DataFrame dans un CSV temporaire (`CSVWithNames`).
8. **Insère** les données du CSV dans ClickHouse via `clickhouse_connect.driver.tools.insert_file`.
9. Supprime les fichiers temporaires.

## 🛠️ Étendre / Personnaliser

- Pour ajouter de nouveaux jeux GISTRID, insérez une entrée dans le dictionnaire `configs` avec :
  - DDL (SQL de création de table)
  - Nom de la table cible
  - Mapping des conversions de colonnes (`data_conversion_expression`)
