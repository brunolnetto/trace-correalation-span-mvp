#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# DATABASE_URL takes precedence. Otherwise use the host port published by
# docker-compose.yml and the same disposable-development defaults as Compose.
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-15432}"
export PGDATABASE="${PGDATABASE:-${POSTGRES_DB:-app}}"
export PGUSER="${PGUSER:-${POSTGRES_USER:-app}}"
export PGPASSWORD="${PGPASSWORD:-${POSTGRES_PASSWORD:-app}}"

python -m app.persistence.migrations
