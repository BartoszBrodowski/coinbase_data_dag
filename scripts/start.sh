#!/usr/bin/env bash

set -Eeuo pipefail

run_compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose "$@"
    elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
    else
        echo "Docker Compose is required. Install Docker Desktop or the Compose plugin." >&2
        return 1
    fi
}

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${repository_root}/.env" ]]; then
    echo "Create .env from .env.example and replace every change-me value first." >&2
    exit 1
fi

cd "${repository_root}"
run_compose config --quiet
run_compose --profile migration run --rm flyway migrate
run_compose up -d --build
run_compose ps
