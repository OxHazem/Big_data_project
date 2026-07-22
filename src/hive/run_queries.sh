#!/bin/bash
# =============================================================================
# run_queries.sh
# Runs each extracted Hive query and exports the results as CSV files
# into data/summary/ on the host machine.
# Each query produces: data/summary/<QueryName>.csv
# =============================================================================

set -uo pipefail

# ── Colors ────────────────────────────────────────────────────────────────────
CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SUMMARY_DIR="$PROJECT_ROOT/data/summary"
CONTAINER_NAME="hive-server"
JDBC_URL="jdbc:hive2://localhost:10000/instacart"

echo -e "${CYAN}========================================${RESET}"
echo -e "${CYAN}📊 Hive Query Export to CSV${RESET}"
echo -e "${CYAN}========================================${RESET}"
echo ""

# ── Preflight checks ──────────────────────────────────────────────────────────
info "Checking if $CONTAINER_NAME is running..."
if ! docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null | grep -q "true"; then
    error "Container '$CONTAINER_NAME' is not running. Start with: docker compose up -d hive-server"
fi
success "Container is running"

info "Checking if HiveServer2 is listening on port 10000..."
if ! docker exec "$CONTAINER_NAME" bash -c "ss -tlnp 2>/dev/null | grep -q 10000"; then
    warn "Port 10000 not bound yet. Waiting up to 90s..."
    for i in $(seq 1 18); do
        sleep 5
        if docker exec "$CONTAINER_NAME" bash -c "ss -tlnp 2>/dev/null | grep -q 10000"; then
            success "Port 10000 is ready"
            break
        fi
        info "[$((i*5))s] Still waiting for HiveServer2..."
        if [ "$i" -eq 18 ]; then
            error "HiveServer2 did not start in 90s. Run: docker logs $CONTAINER_NAME"
        fi
    done
else
    success "HiveServer2 is ready on port 10000"
fi

mkdir -p "$SUMMARY_DIR"
success "Output directory: $SUMMARY_DIR"
echo ""

# ── Query definitions ─────────────────────────────────────────────────────────
# Format: "Filename|HQL"
# NOTE: ORDER BY must use aliases (category/metric_value) because Hive
#       resolves ORDER BY in the outer scope where original col names are gone.
declare -a QUERIES=(
    "Busiest_Hours|SELECT CAST(order_hour_of_day AS STRING) AS category, COUNT(1) AS metric_value FROM orders GROUP BY order_hour_of_day ORDER BY category"
    "Busiest_Days|SELECT CAST(order_dow AS STRING) AS category, COUNT(1) AS metric_value FROM orders GROUP BY order_dow ORDER BY category"
    "Top_10_Products|SELECT p.product_name AS category, COUNT(op.product_id) AS metric_value FROM order_products_prior op JOIN products p ON op.product_id = p.product_id GROUP BY p.product_name ORDER BY metric_value DESC LIMIT 10"
    "Top_Departments|SELECT d.department AS category, COUNT(op.product_id) AS metric_value FROM order_products_prior op JOIN products p ON op.product_id = p.product_id JOIN departments d ON p.department_id = d.department_id GROUP BY d.department ORDER BY metric_value DESC"
    "Reorder_Ratio_Dept|SELECT d.department AS category, ROUND(SUM(CAST(op.reordered AS DOUBLE)) / COUNT(op.product_id), 4) AS metric_value FROM order_products_prior op JOIN products p ON op.product_id = p.product_id JOIN departments d ON p.department_id = d.department_id GROUP BY d.department ORDER BY metric_value DESC"
    "Top_10_Aisles|SELECT a.aisle AS category, COUNT(op.product_id) AS metric_value FROM order_products_prior op JOIN products p ON op.product_id = p.product_id JOIN aisles a ON p.aisle_id = a.aisle_id GROUP BY a.aisle ORDER BY metric_value DESC LIMIT 10"
    "Reorder_Ratio_Aisle|SELECT a.aisle AS category, ROUND(SUM(CAST(op.reordered AS DOUBLE)) / COUNT(op.product_id), 4) AS metric_value FROM order_products_prior op JOIN products p ON op.product_id = p.product_id JOIN aisles a ON p.aisle_id = a.aisle_id GROUP BY a.aisle ORDER BY metric_value DESC"
    "Top_Reordered_Products|SELECT p.product_name AS category, SUM(op.reordered) AS metric_value FROM order_products_prior op JOIN products p ON op.product_id = p.product_id GROUP BY p.product_name ORDER BY metric_value DESC LIMIT 10"
    "Days_Since_Prior_Order|SELECT CAST(days_since_prior_order AS STRING) AS category, COUNT(1) AS metric_value FROM orders WHERE days_since_prior_order IS NOT NULL GROUP BY days_since_prior_order ORDER BY CAST(category AS FLOAT)"
    "Top_10_Users|SELECT CAST(user_id AS STRING) AS category, COUNT(order_id) AS metric_value FROM orders GROUP BY user_id ORDER BY metric_value DESC LIMIT 10"
)

# ── Run each query ────────────────────────────────────────────────────────────
PASS=0
FAIL=0
FAILED_QUERIES=()

TOTAL=${#QUERIES[@]}
echo -e "${CYAN}Running $TOTAL queries...${RESET}"
echo ""

for entry in "${QUERIES[@]}"; do
    NAME="${entry%%|*}"
    HQL="${entry#*|}"
    OUT_FILE="$SUMMARY_DIR/${NAME}.csv"

    info "[$NAME] Executing..."

    RAW_OUTPUT=$(docker exec "$CONTAINER_NAME" beeline \
        --silent=true \
        --outputformat=csv2 \
        --showHeader=true \
        --fastConnect=true \
        -u "$JDBC_URL" \
        -e "$HQL" 2>&1) || true

    # Strip all non-data lines:
    #   - SLF4J warnings
    #   - Beeline connection banner lines
    #   - Beeline prompt lines (e.g. "0: jdbc:hive2://...>")
    #   - Error lines (captured separately for reporting)
    #   - Blank lines
    CSV_OUTPUT=$(printf '%s\n' "$RAW_OUTPUT" \
        | grep -v '^SLF4J' \
        | grep -v '^Connecting' \
        | grep -v '^Connected' \
        | grep -v '^Driver' \
        | grep -v '^Transaction' \
        | grep -v '^Beeline' \
        | grep -v '^Closing' \
        | grep -v '^[0-9]*: jdbc:' \
        | grep -v '^[[:space:]]*$') || true

    # Check if there was an error in the raw output
    ERROR_LINE=$(printf '%s\n' "$RAW_OUTPUT" | grep -i 'Error:' | head -1) || true

    if [ -n "$ERROR_LINE" ]; then
        warn "[$NAME] ⚠  Query failed: $ERROR_LINE"
        echo "category,metric_value" > "$OUT_FILE"
        FAIL=$((FAIL + 1))
        FAILED_QUERIES+=("$NAME")
    elif [ -z "$CSV_OUTPUT" ]; then
        warn "[$NAME] ⚠  Query returned no data (table may be empty)"
        echo "category,metric_value" > "$OUT_FILE"
        FAIL=$((FAIL + 1))
        FAILED_QUERIES+=("$NAME")
    else
        printf '%s\n' "$CSV_OUTPUT" > "$OUT_FILE"
        ROW_COUNT=$(printf '%s\n' "$CSV_OUTPUT" | tail -n +2 | wc -l)
        success "[$NAME] ✅  $ROW_COUNT rows → ${NAME}.csv"
        PASS=$((PASS + 1))
    fi
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}========================================${RESET}"
echo -e "${GREEN}✅ Done: $PASS/$TOTAL queries exported${RESET}"
if [ "$FAIL" -gt 0 ]; then
    echo -e "${YELLOW}⚠  Failed/empty: $FAIL queries${RESET}"
    for q in "${FAILED_QUERIES[@]}"; do
        echo -e "   ${RED}• $q${RESET}"
    done
fi
echo -e "${CYAN}Output: $SUMMARY_DIR${RESET}"
echo -e "${CYAN}========================================${RESET}"
