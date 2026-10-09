#!/usr/bin/env bash
# Deploy the docker-jobs stack by hand, from a swarm manager:
#
#   ./deploy.sh check      validate .env, the token file and the compose file
#   ./deploy.sh deploy     create the token secret if new, then deploy
#   ./deploy.sh status     the stack's services, and the jobs running now
#
# The API token is a swarm secret named after a hash of its content: a new
# token is a new secret, and the backend rolls onto it.
#
# DOCKER_CONTEXT decides WHICH swarm. Check it before deploying.
set -euo pipefail

STACK="${STACK:-docker-jobs}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

TOKEN=secrets/api-token

REQUIRED=(
    DJ_IMAGE_REGISTRY DJ_BACKEND_VERSION DJ_APP_VERSION SOCKET_PROXY_IMAGE DJ_RUN_STACK
    DJ_MAX_RUNNING MONITORING_NETWORK OTEL_EXPORTER_OTLP_ENDPOINT
)

die() { echo "ERROR: $*" >&2; exit 1; }

load_env() {
    [ -f .env ] || die "No .env here -- cp .env.example .env and fill it in."
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a

    local name missing=()
    for name in "${REQUIRED[@]}"; do
        [ -n "${!name:-}" ] || missing+=("$name")
    done
    [ ${#missing[@]} -eq 0 ] || die "Empty in .env: ${missing[*]}"
}

check_files() {
    [ -s "$TOKEN" ] || die "No $TOKEN -- create it: openssl rand -hex 32 > $TOKEN"
}

# name_for <prefix> <file>: a content-addressed object name
name_for() {
    echo "${1}_$(sha256sum "$2" | cut -c1-12)"
}

ensure_secret() {
    if docker secret inspect "$1" > /dev/null 2>&1; then
        echo "  secret $1 exists"
    else
        docker secret create --label "com.docker.stack.namespace=$STACK" "$1" "$2" > /dev/null
        echo "  secret $1 created"
    fi
}

objects() {
    DJ_API_TOKEN_SECRET_NAME="$(name_for "${STACK//-/_}_api_token" "$TOKEN")"
    export DJ_API_TOKEN_SECRET_NAME
}

cmd_check() {
    load_env
    check_files
    objects
    docker stack config -c docker-compose.yml > /dev/null
    echo "OK: .env, the token and docker-compose.yml are consistent"
    echo "Target: DOCKER_CONTEXT=${DOCKER_CONTEXT:-$(docker context show)}"
}

cmd_deploy() {
    cmd_check
    docker network inspect "$MONITORING_NETWORK" > /dev/null 2>&1 \
        || die "External network $MONITORING_NETWORK does not exist on this swarm."
    [ -n "$(docker node ls --filter node.label=docker-jobs=true -q)" ] \
        || die "No node labelled docker-jobs=true: docker node update --label-add docker-jobs=true <node>"
    echo "Swarm objects:"
    ensure_secret "$DJ_API_TOKEN_SECRET_NAME" "$TOKEN"
    docker stack deploy -c docker-compose.yml --prune --with-registry-auth \
        --resolve-image changed --detach=false "$STACK"
}

cmd_status() {
    load_env
    docker stack services "$STACK"
    docker stack ps --no-trunc --format 'table {{.Name}}\t{{.CurrentState}}\t{{.Error}}' "$STACK" | head -30
    echo "Jobs running now ($DJ_RUN_STACK):"
    docker service ls --filter "label=com.docker.stack.namespace=$DJ_RUN_STACK"
}

case "${1:-}" in
    check)  cmd_check ;;
    deploy) cmd_deploy ;;
    status) cmd_status ;;
    *) sed -n '2,12p' "$0"; exit 2 ;;
esac
