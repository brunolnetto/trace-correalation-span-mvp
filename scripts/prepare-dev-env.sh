#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
touch "$ENV_FILE"
chmod 600 "$ENV_FILE"

ensure_secret() {
  local name="$1"
  if ! grep -q "^${name}=." "$ENV_FILE"; then
    local value
    value="$(python3 -c 'import secrets; print(secrets.token_urlsafe(36))')"
    printf '%s=%s\n' "$name" "$value" >> "$ENV_FILE"
  fi
}

for name in \
  KEYCLOAK_ADMIN_PASSWORD \
  ORDER_API_CLIENT_SECRET \
  INVENTORY_API_CLIENT_SECRET \
  SUPPLIER_SIMULATOR_CLIENT_SECRET \
  EVENT_COLLECTOR_CLIENT_SECRET \
  OUTBOX_DISPATCHER_CLIENT_SECRET \
  ORDER_GENERATOR_CLIENT_SECRET \
  STANDALONE_RUNNER_CLIENT_SECRET \
  LOCAL_DEVELOPER_CLIENT_SECRET \
  LOCAL_DEVELOPER_PASSWORD
do
  ensure_secret "$name"
done

echo "    generated/loaded local-only credentials from ignored .env"
