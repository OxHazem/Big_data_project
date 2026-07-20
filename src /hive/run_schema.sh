#!/bin/bash
# =============================================================================
# run_schema.sh
# Executes the Hive schema creation script inside the hive-server container.
# Automatically detects if the server is down and attempts to restart it.
# =============================================================================

set -euo pipefail

# Define colors for output
CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

# Resolve paths dynamically
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
HQL_FILE="$SCRIPT_DIR/create_schema.hql"
CONTAINER_NAME="hive-server"
JDBC_URL="jdbc:hive2://localhost:10000"

echo -e "${CYAN}========================================${RESET}"
echo -e "${CYAN}🐝 Starting Hive Schema Deployment...${RESET}"
echo -e "${CYAN}========================================${RESET}"

# 1. Check if the HQL schema file exists
if [ ! -f "$HQL_FILE" ]; then
    error "Schema file not found!\nExpected to find: $HQL_FILE\n(Make sure your file is named exactly 'create_schema.hql')"
fi
success "Found schema file: $HQL_FILE"

# 2. Check if the Hive container is running, start it if it isn't
info "Checking if $CONTAINER_NAME is active..."
if ! docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null | grep -q "true"; then
    warn "Container '$CONTAINER_NAME' is down. Attempting to start it..."
    
    # Move to the project root to run docker compose
    cd "$PROJECT_ROOT"
    docker compose up -d "$CONTAINER_NAME"
    
    info "Waiting 15 seconds for Hive to initialize and connect to HDFS..."
    sleep 15
    
    # Re-check the status to ensure it didn't crash again
    if ! docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null | grep -q "true"; then
        error "'$CONTAINER_NAME' crashed again! Run 'docker logs $CONTAINER_NAME' to read the error."
    fi
fi
success "Hive server is running"

# 3. Execute the SQL script via Beeline
info "Streaming SQL to Hive Beeline..."
echo ""

if cat "$HQL_FILE" | docker exec -i "$CONTAINER_NAME" beeline -u "$JDBC_URL"; then
    echo ""
    echo -e "${GREEN}========================================${RESET}"
    echo -e "${GREEN}✅ Hive schema deployed successfully!${RESET}"
    echo -e "${GREEN}========================================${RESET}"
else
    echo ""
    error "Deployment failed. Check the Hive logs above for SQL syntax errors."
fi