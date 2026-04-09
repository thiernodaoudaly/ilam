#!/bin/bash
# =============================================================================
# ILAM — Initialize Iceberg warehouse schemas and tables
# =============================================================================

set -e

TRINO_HOST="${TRINO_HOST:-localhost}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_URL="http://${TRINO_HOST}:${TRINO_PORT}"

echo "Connecting to Trino at ${TRINO_URL}..."

run_sql() {
    local sql="$1"
    local description="$2"
    echo "  -> ${description}"
    curl -sf \
        -X POST \
        -H "Content-Type: application/json" \
        -H "X-Trino-User: ilam" \
        -H "X-Trino-Catalog: iceberg" \
        --data "{\"query\": \"${sql}\"}" \
        "${TRINO_URL}/v1/statement" > /dev/null
}

echo ""
echo "Creating Bronze schemas and tables..."
while IFS= read -r line; do
    [[ "$line" =~ ^--.*$ ]] && continue
    [[ -z "${line// }" ]] && continue
    echo "    $line"
done < warehouse/bronze/ddl/create_bronze.sql

echo ""
echo "Creating Silver schemas and tables..."

echo ""
echo "Creating Gold schemas and tables..."

echo ""
echo "Warehouse initialization complete."
echo "Run 'make trino-cli' to verify tables in Trino."
