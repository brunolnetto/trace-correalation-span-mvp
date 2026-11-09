#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

KCADM_CONFIG="/tmp/keycloak-admin-$BASHPID.config"
KCADM=(/opt/keycloak/bin/kcadm.sh --config "$KCADM_CONFIG")
REALM="trace-local"
SERVER="http://keycloak:8080"
TEMP_FILES=("$KCADM_CONFIG")

cleanup_sensitive_files() {
  local file
  for file in "${TEMP_FILES[@]}"; do
    if [[ -f "$file" ]]; then
      : > "$file"
      rm -f "$file"
    fi
  done
}
trap cleanup_sensitive_files EXIT

if ! "${KCADM[@]}" config credentials \
  --server "$SERVER" \
  --realm master \
  --user "$KC_BOOTSTRAP_ADMIN_USERNAME" \
  --password "$KC_BOOTSTRAP_ADMIN_PASSWORD"; then
  echo "Keycloak administrative authentication failed after readiness check" >&2
  exit 1
fi

if ! "${KCADM[@]}" get "realms/$REALM" >/dev/null 2>&1; then
  "${KCADM[@]}" create realms -s "realm=$REALM" -s enabled=true
fi

ALL_AUDIENCES="order-api inventory-api supplier-simulator event-collector outbox-dispatcher"
ALL_SCOPES="catalog:read catalog:write orders:read orders:process inventory:read inventory:reserve inventory:fulfill inventory:receive inventory:adjust suppliers:read suppliers:manage suppliers:evaluate simulation:control events:ingest events:read operations:read"

ensure_scope() {
  local scope="$1"
  local scope_file json_file scopes_json scope_id line

  scope_file="${scope//:/_}"
  json_file="/tmp/keycloak-scope-${scope_file}-$BASHPID.json"
  TEMP_FILES+=("$json_file")
  {
    printf '{\n'
    printf '  "name": "%s",\n' "$scope"
    printf '  "protocol": "openid-connect",\n'
    printf '  "attributes": {\n'
    printf '    "include.in.token.scope": "true",\n'
    printf '    "display.on.consent.screen": "false"\n'
    printf '  }\n'
    printf '}\n'
  } > "$json_file"

  scopes_json="$("${KCADM[@]}" get client-scopes -r "$REALM" 2>/dev/null || true)"
  scope_id=""
  current_id=""
  current_name=""
  while IFS= read -r line; do
    if [[ "$line" == *"{"* ]]; then
      current_id=""
      current_name=""
    fi
    if [[ "$line" =~ \"id\"[[:space:]]*:[[:space:]]*\"([^\"]+)\" ]]; then
      current_id="${BASH_REMATCH[1]}"
    fi
    if [[ "$line" =~ \"name\"[[:space:]]*:[[:space:]]*\"([^\"]+)\" ]]; then
      current_name="${BASH_REMATCH[1]}"
    fi
    if [[ -n "$current_id" && "$current_name" == "$scope" ]]; then
      scope_id="$current_id"
      break
    fi
  done <<< "$scopes_json"

  if [[ -n "$scope_id" ]]; then
    if ! "${KCADM[@]}" update "client-scopes/$scope_id" -r "$REALM" -f "$json_file"; then
      return 1
    fi
  else
    if ! "${KCADM[@]}" create client-scopes -r "$REALM" -f "$json_file"; then
      return 1
    fi
  fi
}

ensure_client() {
  local client="$1"
  local secret="$2"
  local scopes="$3"
  local audiences="$4"
  local json_file client_uuid clients_json line

  json_file="/tmp/keycloak-client-${client}-$BASHPID.json"
  TEMP_FILES+=("$json_file")
  {
    printf '{\n'
    printf '  "clientId": "%s",\n' "$client"
    printf '  "name": "Local development identity: %s",\n' "$client"
    printf '  "description": "Development-only OAuth2 client. Never use these credentials outside local Compose.",\n'
    printf '  "enabled": true,\n'
    printf '  "protocol": "openid-connect",\n'
    printf '  "publicClient": false,\n'
    printf '  "clientAuthenticatorType": "client-secret",\n'
    printf '  "secret": "%s",\n' "$secret"
    printf '  "serviceAccountsEnabled": true,\n'
    printf '  "standardFlowEnabled": false,\n'
    printf '  "directAccessGrantsEnabled": false,\n'
    printf '  "implicitFlowEnabled": false,\n'
    printf '  "optionalClientScopes": ['
    local first=1 scope
    for scope in $scopes; do
      if (( first )); then first=0; else printf ', '; fi
      printf '"%s"' "$scope"
    done
    printf '],\n'
    printf '  "protocolMappers": [\n'

    local first=1 audience
    for audience in $audiences; do
      if (( first )); then first=0; else printf ',\n'; fi
      printf '    {"name":"audience-%s","protocol":"openid-connect","protocolMapper":"oidc-audience-mapper","consentRequired":false,"config":{"included.client.audience":"%s","id.token.claim":"false","access.token.claim":"true","introspection.token.claim":"true"}}' "$audience" "$audience"
    done
    printf '\n  ]\n'
    printf '}\n'
  } > "$json_file"

  clients_json="$("${KCADM[@]}" get clients -r "$REALM" -q "clientId=$client" 2>/dev/null || true)"
  client_uuid=""
  while IFS= read -r line; do
    if [[ "$line" =~ \"id\"[[:space:]]*:[[:space:]]*\"([^\"]+)\" ]]; then
      client_uuid="${BASH_REMATCH[1]}"
      break
    fi
  done <<< "$clients_json"

  if [[ -n "$client_uuid" ]]; then
    if ! "${KCADM[@]}" update "clients/$client_uuid" -r "$REALM" -f "$json_file"; then
      return 1
    fi
  else
    if ! "${KCADM[@]}" create clients -r "$REALM" -f "$json_file"; then
      return 1
    fi
  fi
}

for scope in $ALL_SCOPES; do
  ensure_scope "$scope"
done

# First create every client without audience references. Audience mapper client IDs
# must already exist in the realm before the second pass configures those mappers.
ensure_client order-api "$ORDER_API_CLIENT_SECRET" \\
  "catalog:read inventory:read inventory:reserve inventory:fulfill events:ingest" ""
ensure_client inventory-api "$INVENTORY_API_CLIENT_SECRET" "events:ingest" ""
ensure_client supplier-simulator "$SUPPLIER_SIMULATOR_CLIENT_SECRET" \\
  "catalog:read catalog:write inventory:read inventory:receive events:ingest" ""
ensure_client event-collector "$EVENT_COLLECTOR_CLIENT_SECRET" "events:ingest" ""
ensure_client outbox-dispatcher "$OUTBOX_DISPATCHER_CLIENT_SECRET" "events:ingest" ""
ensure_client order-generator "$ORDER_GENERATOR_CLIENT_SECRET" \\
  "orders:process events:ingest" ""
ensure_client standalone-runner "$STANDALONE_RUNNER_CLIENT_SECRET" \\
  "catalog:read orders:process inventory:read inventory:reserve inventory:fulfill inventory:receive suppliers:read suppliers:manage suppliers:evaluate simulation:control events:ingest events:read" ""
ensure_client local-developer "$LOCAL_DEVELOPER_CLIENT_SECRET" "$ALL_SCOPES" ""

ensure_client order-api "$ORDER_API_CLIENT_SECRET" \
  "catalog:read inventory:read inventory:reserve inventory:fulfill events:ingest" \
  "inventory-api event-collector"
ensure_client inventory-api "$INVENTORY_API_CLIENT_SECRET" \
  "events:ingest" \
  "event-collector"
ensure_client supplier-simulator "$SUPPLIER_SIMULATOR_CLIENT_SECRET" \
  "catalog:read catalog:write inventory:read inventory:receive events:ingest" \
  "inventory-api event-collector"
ensure_client event-collector "$EVENT_COLLECTOR_CLIENT_SECRET" \
  "events:ingest" \
  ""
ensure_client outbox-dispatcher "$OUTBOX_DISPATCHER_CLIENT_SECRET" \
  "events:ingest" \
  "event-collector"
ensure_client order-generator "$ORDER_GENERATOR_CLIENT_SECRET" \
  "orders:process events:ingest" \
  "order-api event-collector"
ensure_client standalone-runner "$STANDALONE_RUNNER_CLIENT_SECRET" \
  "catalog:read orders:process inventory:read inventory:reserve inventory:fulfill inventory:receive suppliers:read suppliers:manage suppliers:evaluate simulation:control events:ingest events:read" \
  "inventory-api order-api supplier-simulator event-collector"
ensure_client local-developer "$LOCAL_DEVELOPER_CLIENT_SECRET" "$ALL_SCOPES" "$ALL_AUDIENCES"

echo "Configured local OIDC realm '$REALM' and independent service clients."
