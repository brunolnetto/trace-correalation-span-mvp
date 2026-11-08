#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MIGRATION_VERSION="0001_core_persistence"
MIGRATION_FILE="migrations/0001_core_persistence.sql"
POSTGRES_USER="${POSTGRES_USER:-app}"
POSTGRES_DB="${POSTGRES_DB:-app}"
POSTGRES_SERVICE="${POSTGRES_SERVICE:-postgres}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: Docker CLI is required." >&2
  exit 1
fi
if ! command -v sha256sum >/dev/null 2>&1; then
  echo "ERROR: sha256sum is required." >&2
  exit 1
fi
if [[ ! -f "$MIGRATION_FILE" ]]; then
  echo "ERROR: migration file not found: $MIGRATION_FILE" >&2
  exit 1
fi

checksum="$(sha256sum "$MIGRATION_FILE" | cut -d ' ' -f 1)"
wrapper="$(mktemp)"
trap 'rm -f "$wrapper"' EXIT

cat >"$wrapper" <<SQL
\\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(210021);
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(128) PRIMARY KEY,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SELECT EXISTS (
    SELECT 1 FROM schema_migrations WHERE version = '$MIGRATION_VERSION'
) AS already_applied \\gset
\\if :already_applied
    SELECT checksum = :'migration_checksum' AS checksum_matches
    FROM schema_migrations
    WHERE version = '$MIGRATION_VERSION' \\gset
    \\if :checksum_matches
        \\echo "Migration $MIGRATION_VERSION already applied; checksum matches."
    \\else
        \\echo "ERROR: checksum mismatch for applied migration $MIGRATION_VERSION."
        \\quit 3
    \\endif
\\else
SQL
cat "$MIGRATION_FILE" >>"$wrapper"
cat >>"$wrapper" <<SQL
INSERT INTO schema_migrations (version, checksum)
VALUES ('$MIGRATION_VERSION', :'migration_checksum');
\\echo "Applied migration $MIGRATION_VERSION."
\\endif
COMMIT;
SQL

docker compose exec -T \
  -e PGPASSWORD="${POSTGRES_PASSWORD:-app}" \
  "$POSTGRES_SERVICE" \
  psql -v ON_ERROR_STOP=1 \
    -v migration_checksum="$checksum" \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -f - < "$wrapper"
