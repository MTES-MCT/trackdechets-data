#!/bin/bash
uv run --env-file dbt/.env sqlfluff fix
uv run --env-file ../.env dbt-osmosis yaml refactor --auto-apply