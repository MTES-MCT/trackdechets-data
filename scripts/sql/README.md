# Run migrations

These SQL files must be run against Clickhouse Datawarehouse or Trackdechet DB.
SSH tunnel must be active to run these queries against Production or Sandbox databases.

## Example 
```bash
uv run python run_migrations.py --file create_empty_zammad_tables.sql --DB_HOST localhost --DB_PORT 18123 --DB_USER user --DB_PASSWORD password
```

