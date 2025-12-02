#!/usr/bin/env python3
import argparse
import json
import os
from dataclasses import dataclass
from typing import Dict, List, Set
import clickhouse_connect
from pathlib import Path
from dotenv import load_dotenv

CURRENT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


@dataclass(frozen=True)
class Relation:
    schema: str
    name: str
    type: str  # table/view

    def __str__(self) -> str:
        return f"{self.schema}.{self.name} ({self.type})"


def load_dbt_relations(
    manifest_path: str, schema_filter: Set[str] | None = None
) -> Set[Relation]:
    """Charge les relations (tables/vues) gérées par dbt à partir du manifest.json."""
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"manifest.json introuvable: {manifest_path}")

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    nodes: Dict[str, Dict] = manifest.get("nodes", {})
    sources: Dict[str, Dict] = manifest.get("sources", {})

    relations: Set[Relation] = set()

    def add_node(node: Dict):
        # On ne prend que les modèles matériels (= relations physiques)
        materialized = node.get("config", {}).get("materialized")
        if materialized not in {"table", "view", "incremental"}:
            return

        relation_name = node.get("alias") or node.get("name")
        relation_schema = node.get("schema") or node.get("fqn", [None])[0]
        if not relation_name or not relation_schema:
            return

        relation_type = "table" if materialized in {"table", "incremental"} else "view"

        if schema_filter and relation_schema not in schema_filter:
            return

        relations.add(
            Relation(schema=relation_schema, name=relation_name, type=relation_type)
        )

    # modèles dbt
    for node in nodes.values():
        if node.get("resource_type") == "model":
            add_node(node)

    # (optionnel) sources dbt si on veut les considérer comme "attendues"
    for src in sources.values():
        if src.get("resource_type") == "source":
            relation_name = src.get("identifier") or src.get("name")
            relation_schema = src.get("schema")
            if not relation_name or not relation_schema:
                continue
            if schema_filter and relation_schema not in schema_filter:
                continue
            # On ne connaît pas toujours le type exact : on laisse "table"
            relations.add(
                Relation(schema=relation_schema, name=relation_name, type="table")
            )

    return relations


def load_dwh_relations(
    client,
    schema_filter: Set[str],
    include_views: bool = True,
    include_tables: bool = True,
) -> Set[Relation]:
    """
    Récupère toutes les tables/vues du DWH dans les schémas ciblés (ClickHouse).
    """
    rels: Set[Relation] = set()
    reltypes: List[str] = []
    if include_tables:
        reltypes.append("TABLE")
    if include_views:
        reltypes.append("VIEW")
        reltypes.append("MATERIALIZED VIEW")

    # ClickHouse system.tables (ou system.objects pour plus de détails avec les vues matérialisées)
    schema_filter_list = list(schema_filter)
    schema_placeholders = ", ".join([f"'{s}'" for s in schema_filter_list])
    sql = f"""
        SELECT
            database as table_schema,
            name as table_name,
            engine
        FROM system.tables
        WHERE database IN ({schema_placeholders})
    """

    # Note: We'll distinguish table/view/materialized view via engine
    # For views, might need to check system.views as well, but keep simple for now.

    rows = client.query(sql).named_results()

    for r in rows:
        schema = r["table_schema"]
        name = r["table_name"]
        engine = r["engine"]
        # Infer type from ClickHouse's engine
        if engine in ("View", "LiveView"):
            rel_type = "view"
        elif engine in ("MaterializedView",):
            rel_type = "view"
        else:
            rel_type = "table"
        # Only include the requested types
        if (rel_type == "table" and include_tables) or (
            rel_type == "view" and include_views
        ):
            rels.add(Relation(schema=schema, name=name, type=rel_type))
    return rels


def compute_deprecated(
    dbt_relations: Set[Relation],
    dwh_relations: Set[Relation],
) -> Set[Relation]:
    """
    Objets dépréciés = présents dans le DWH mais absents du manifest dbt.
    (on compare sur schema + name, on ignore le type pour la comparaison de base)
    """
    dbt_keys = {(r.schema, r.name) for r in dbt_relations}
    deprecated = {r for r in dwh_relations if (r.schema, r.name) not in dbt_keys}
    return deprecated


def drop_relations(client, relations: Set[Relation], dry_run: bool = True):
    """
    Supprime les relations obsolètes. En mode dry-run, se contente de les afficher.
    """
    if not relations:
        print("Aucune relation dépréciée à supprimer.")
        return

    print(f"\nRelations dépréciées détectées ({len(relations)}):")
    for r in sorted(relations, key=lambda x: (x.schema, x.name)):
        print(f"  - {r}")

    if dry_run:
        print("\nMode dry-run activé : aucune suppression n'a été effectuée.")
        return

    print("\nSuppression des relations dépréciées...")
    for r in sorted(relations, key=lambda x: (x.schema, x.name)):
        # On utilise IF EXISTS pour être plus robuste
        if r.type == "view":
            sql = f"DROP VIEW IF EXISTS `{r.schema}`.`{r.name}`;"
        else:
            sql = f"DROP TABLE IF EXISTS `{r.schema}`.`{r.name}`;"
        print(f"  Exécution : {sql}")
        # ClickHouse doesn't support multiple statements in one query by default
        for stmt in sql.strip().split("\n"):
            client.command(stmt)
    print("Suppression terminée.")


def main():
    parser = argparse.ArgumentParser(
        description="Nettoyage des tables/vues obsolètes (dbt vs DWH, ClickHouse)."
    )
    parser.add_argument(
        "--dry-run",
        required=True,
        type=lambda x: (False if str(x).lower() == "false" else True),
        help="If true, no deletion will be performed, only the deprecated objects will be listed.",
    )
    parser.add_argument(
        "--environment",
        required=True,
        help="Environment to clean obsolete relations from.",
        choices=["dev", "sandbox", "prod"],
    )

    args = parser.parse_args()

    # Charger la liste des schémas à partir du fichier dbt_project.yml dans la section models:trackdechets, clé +schema
    import yaml

    dbt_project_file = Path(CURRENT_DIR.parent.parent, "dbt", "dbt_project.yml")
    with open(dbt_project_file, "r") as f:
        dbt_project = yaml.safe_load(f)

    # Naviguer vers models:trackdechets
    trackdechets_models = dbt_project.get("models", {}).get("trackdechets", {})
    # Certains schémas peuvent être listés sous plusieurs sous-sections, mais on prend les +schema définis directement
    schema_filter = list[str]()
    for zone, zone_config in trackdechets_models.items():
        if isinstance(zone_config, str):
            continue
        for schema_name, schema_config in zone_config.items():
            if "+schema" in schema_config:
                schema_filter.append(schema_config["+schema"])

    if not schema_filter:
        raise ValueError(
            "Aucun schéma trouvé dans dbt_project.yml sous models:trackdechets:+schema"
        )

    print(f"Schémas ciblés : {', '.join(sorted(schema_filter))}")

    manifest_path = Path(CURRENT_DIR.parent.parent, "dbt", "target", "manifest.json")

    print(f"Chargement des relations dbt depuis : {manifest_path}")

    dbt_relations = load_dbt_relations(manifest_path, schema_filter=schema_filter)
    print(f"Relations gérées par dbt : {len(dbt_relations)}")

    # Charger les variables d'environnement depuis dbt/.env
    env_file = Path(CURRENT_DIR.parent.parent, "dbt", ".env")
    load_dotenv(env_file)

    connection_params = dict(
        host=os.getenv(f"DBT_HOST_{args.environment.upper()}"),
        port=int(os.getenv(f"DBT_PORT_{args.environment.upper()}")),
        username=os.getenv(f"DBT_USER_{args.environment.upper()}"),
        password=os.getenv(f"DBT_PASSWORD_{args.environment.upper()}"),
    )
    client = clickhouse_connect.get_client(**connection_params)

    try:
        dwh_relations = load_dwh_relations(client, schema_filter=schema_filter)
        print(f"Relations présentes dans le DWH : {len(dwh_relations)}")

        deprecated = compute_deprecated(dbt_relations, dwh_relations)
        drop_relations(client, deprecated, dry_run=args.dry_run)
    finally:
        client.close()


if __name__ == "__main__":
    main()
