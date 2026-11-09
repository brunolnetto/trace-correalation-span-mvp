#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

KCADM="/opt/keycloak/bin/kcadm.sh"
REALM="trace-local"
SERVER="http://keycloak:8080"

configured=0
attempt=0
while (( attempt < 60 )); do
  if "$KCADM" config credentials \
    --server "$SERVER" \
    --realm master \
    --user "$KC_BOOTSTRAP_ADMIN_USERNAME" \
    --password "$KC_BOOTSTRAP_ADMIN_PASSWORD" >/dev/null 2>&1; then
    configured=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done
if (( ! configured )); then
  echo "Keycloak did not become ready for administrative provisioning" >&2
  exit 1
fi

if ! "$KCADM" get "realms/$REALM" >/dev/null 2>&1; then
  "$KCADM" create realms -s "realm=$REALM" -s enabled=true
fi

ALL_AUDIENCES="order-api inventory-api supplier-simulator event-collector outbox-dispatcher"
ALL_SCOPES="catalog:read catalog:write orders:read orders:process inventory:read inventory:reserve inventory:fulfill inventory:receive inventory:adjust suppliers:read suppliers:manage suppliers:evaluate simulation:control events:ingest events:read operations:read"

ensure_client() {
  local client="$1"
  local secret="$2"
  local scopes="$3"
  local audiences="$4"
  local json_file client_uuid clients_json line

  json_file="/tmp/keycloak-client-${client}-$.json"
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
    printf '  "protocolMappers": [\n'
    printf '    {"name":"api-scope-claim","protocol":"openid-connect","protocolMapper":"oidc-hardcoded-claim-mapper","consentRequired":false,"config":{"claim.name":"scope","claim.value":"%s","jsonType.label":"String","access.token.claim":"true","introspection.token.claim":"true","id.token.claim":"false","userinfo.token.claim":"false"}},\n' "$scopes"

    local first=1 audience
    for audience in $audiences; do
      if (( first )); then first=0; else printf ',\n'; fi
      printf '    {"name":"audience-%s","protocol":"openid-connect","protocolMapper":"oidc-audience-mapper","consentRequired":false,"config":{"included.client.audience":"%s","id.token.claim":"false","access.token.claim":"true","introspection.token.claim":"true"}}' "$audience" "$audience"
    done
    printf '\n  ]\n'
    printf '}\n'
  } > "$json_file"

  clients_json="$("$KCADM" get clients -r "$REALM" -q "clientId=$client" 2>/dev/null || true)"
  client_uuid=""
  while IFS= read -r line; do
    if [[ "$line" =~ \"id\"[[:space:]]*:[[:space:]]*\"([^\"]+)\" ]]; then
      client_uuid="${BASH_REMATCH[1]}"
      break
    fi
  done <<< "$clients_json"

  if [[ -n "$client_uuid" ]]; then
    if ! "$KCADM" update "clients/$client_uuid" -r "$REALM" -f "$json_file"; then
      rm -f "$json_file"
      return 1
    fi
  else
    if ! "$KCADM" create clients -r "$REALM" -f "$json_file"; then
      rm -f "$json_file"
      return 1
    fi
  fi
  rm -f "$json_file"
}

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
