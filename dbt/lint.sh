#!/bin/bash
uv run --env-file .env sqlfluff fix
uv run --env-file .env dbt-osmosis yaml refactor