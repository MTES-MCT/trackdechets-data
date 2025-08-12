# Script d'intégration des données GUN dans ClickHouse

Ce script (`load_gun_data_ch.py`) charge un fichier CSV contenant les données de la base **GUN** dans une table ClickHouse située dans la base de données `raw_zone_icpe`.  
Il est conçu pour être exécuté depuis la ligne de commande et accepte plusieurs options de connexion.

## Prérequis

Toutes les bibliothèques requises figurent dans le `pyproject.toml` du projet.  
Installez‑les avec **uv** (le gestionnaire de paquets recommandé pour ce dépôt) :

```bash
uv sync
```

## Utilisation

```bash
uv run scripts/integration/gun/load_gun_data_ch.py \
  --dwh_host <hôte> \
  --dwh_http_port <port> \
  --dwh_username <nom_utilisateur> \
  --dwh_password <mot_de_passe> \
  <chemin_vers_csv>
```

### Options

| Argument          | Valeur par défaut | Description                              |
| ----------------- | ----------------- | ---------------------------------------- |
| `--dwh_host`      | `localhost`       | Hôte ClickHouse                          |
| `--dwh_http_port` | `8123`            | Port HTTP de ClickHouse                  |
| `--dwh_username`  | `default`         | Nom d’utilisateur                        |
| `--dwh_password`  | _(vide)_          | Mot‑de‑passe (laisser vide si aucun)     |
| `filepath`        | **obligatoire**   | Chemin vers le fichier CSV GUN à charger |

## Fonctionnement

1. **Connexion** : Le script se connecte à ClickHouse via `clickhouse_connect`.
2. **Création de la table** : Si elle n’existe pas, la table  
   `raw_zone_icpe.installations_rubriques_2024` est créée avec le DDL fourni.
3. **Pré‑traitement** :
   - Le fichier CSV est lu dans un DataFrame Polars.
   - Des tratiements sont effectués pour que les données soient prêtes à être insérées dans ClickHouse.
4. **Export temporaire** : Le DataFrame est écrit dans un fichier CSV temporaire (`gun.csv`).
5. **Insertion** : Utilisation de `insert_file` pour charger les données en bloc dans ClickHouse.
6. **Nettoyage** : Le répertoire temporaire est supprimé après l’insertion.

## Points à retenir

- Le script attend un fichier CSV où chaque colonne correspond exactement aux champs décrits dans le DDL.
- Les colonnes `Capacité projet` et `Capacité totale` sont automatiquement renommées en `Capacité Projet` et `Capacité Totale`.
- La colonne `_inserted_at` est remplie par la fonction `now('Europe/Paris')` lors de l’insertion.

## Débogage

- **Logs** : Le script utilise le module `logging`. Vous verrez les étapes dans la console.
- **Erreurs de connexion** : Vérifiez que votre instance ClickHouse écoute sur le port HTTP spécifié et que vos identifiants sont corrects.
- **Données manquantes** : Si des colonnes attendues ne sont pas présentes, le script lèvera une exception lors du chargement.
