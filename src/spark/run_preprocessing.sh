#!/bin/bash
# =============================================================================
# run_preprocessing.sh
# Submits the PySpark preprocessing job to the Spark cluster running
# inside Docker Compose (big_data_workspace → spark-master:7077).
#
# Usage (from the project root):
#   chmod +x "src /spark/run_preprocessing.sh"
#   bash "src /spark/run_preprocessing.sh"           # normal run
#   bash "src /spark/run_preprocessing.sh" --build   # rebuild workspace image first
# =============================================================================

set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
# Resolve the project root (two levels up from src /spark/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"

# Container names — must match docker-compose.yml
WORKSPACE_CONTAINER="big_data_workspace"
NAMENODE_CONTAINER="hadoop-namenode"
SPARK_MASTER_CONTAINER="spark-master"

# Path to preprocess.py inside the workspace container.
# The project root is mounted at /app  (volumes: .:/app in docker-compose.yml)
PREPROCESS_SCRIPT="/app/src/spark/preprocess.py"

# spark-submit resource settings
# NOTE: We use local[*] instead of spark://spark-master:7077 because the
# workspace image uses Spark 3.5.0 (jupyter/pyspark-notebook) while the
# bde2020 cluster images use Spark 3.2.1 — cross-version registration is
# not supported. local[*] uses all cores inside the workspace container
# and still reads/writes HDFS via hdfs://namenode:9000.
SPARK_MASTER="local[*]"
SPARK_DRIVER_MEMORY="4g"

# Max seconds to wait for each service health-check
WAIT_TIMEOUT=120

# ── Parse CLI flags ───────────────────────────────────────────────────────────
BUILD_FLAG=""
for arg in "$@"; do
    case $arg in
        --build) BUILD_FLAG="--build" ;;
        --help|-h)
            echo "Usage: $0 [--build]"
            echo "  --build   Rebuild the pyspark-workspace Docker image before submitting"
            exit 0 ;;
    esac
done

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║     Instacart — PySpark Preprocessing Launcher               ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""
info "Project root  : $PROJECT_ROOT"
info "Compose file  : $COMPOSE_FILE"
info "Spark script  : $PREPROCESS_SCRIPT (inside container)"
echo ""

# ── Step 1: Verify docker & docker compose are available ──────────────────────
info "Checking dependencies..."
command -v docker      >/dev/null 2>&1 || error "docker not found in PATH"
docker compose version >/dev/null 2>&1 || error "'docker compose' plugin not found — install Docker Compose v2"
success "docker and docker compose are available"

# ── Step 2: Start / rebuild the Docker Compose stack ─────────────────────────
info "Starting Docker Compose stack..."
docker compose -f "$COMPOSE_FILE" up -d $BUILD_FLAG
success "Docker Compose stack started"

# ── Step 3: Generic container readiness helper ────────────────────────────────
wait_for_container() {
    local container="$1"
    local label="$2"
    local elapsed=0
    info "Waiting for $label ($container)..."
    until docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q "true"; do
        sleep 2; elapsed=$((elapsed + 2))
        [ $elapsed -ge $WAIT_TIMEOUT ] && \
            error "Timed out waiting for $label after ${WAIT_TIMEOUT}s — check: docker logs $container"
        echo -ne "  ... ${elapsed}s\r"
    done
    echo ""
    success "$label is running"
}

# ── Step 4: Wait for the three critical services ──────────────────────────────
wait_for_container "$NAMENODE_CONTAINER"     "HDFS NameNode"
wait_for_container "$SPARK_MASTER_CONTAINER" "Spark Master"
wait_for_container "$WORKSPACE_CONTAINER"    "PySpark Workspace"

# ── Step 5: Wait for HDFS to exit safe mode ───────────────────────────────────
info "Checking HDFS safe mode..."
SM_ELAPSED=0
until docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
    sleep 3; SM_ELAPSED=$((SM_ELAPSED + 3))
    if [ $SM_ELAPSED -ge $WAIT_TIMEOUT ]; then
        warn "HDFS still in safe mode after ${WAIT_TIMEOUT}s — forcing exit..."
        docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -safemode leave || true
        break
    fi
    echo -ne "  ... safe mode ON (${SM_ELAPSED}s)\r"
done
echo ""
success "HDFS NameNode is out of safe mode"

# ── Step 6: Ensure HDFS output directory exists with correct permissions ──────
info "Ensuring HDFS output directory exists..."
docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p /user/project/data/preprocessed \
    || warn "Output dir may already exist — continuing"
# The workspace container runs as 'jovyan' but the mkdir above is run as root.
# chmod 777 so jovyan can write Parquet output files.
docker exec "$NAMENODE_CONTAINER" hdfs dfs -chmod -R 777 /user/project/data/preprocessed
success "HDFS path ready (permissions set to 777): /user/project/data/preprocessed"

# ── Step 7: Check raw CSV files are present in HDFS ──────────────────────────
info "Verifying raw CSV files in HDFS..."
RAW_FILES=$(docker exec "$NAMENODE_CONTAINER" \
    hdfs dfs -ls /user/project/data/raw/ 2>/dev/null | grep "\.csv" | wc -l)

if [ "$RAW_FILES" -lt 6 ]; then
    warn "Only $RAW_FILES CSV file(s) found in /user/project/data/raw/ (expected 6)."
    warn "Run 'bash \"src /HDFS/init_HDFS.sh\"' first to upload raw data."
    read -rp "Continue anyway? [y/N] " CONT
    [[ "$CONT" =~ ^[Yy]$ ]] || exit 1
else
    success "Found $RAW_FILES CSV file(s) in HDFS raw directory ✓"
fi

# ── Step 8: Submit spark-submit inside the workspace container ────────────────
echo ""
info "Submitting PySpark job to spark://spark-master:7077 ..."
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

# --- 1. CHANGE THE MASTER URL ---
# (Around line 33)
SPARK_MASTER="spark://spark-master:7077"


# --- 2. CHANGE THE DOCKER EXEC TARGET ---
# (Around line 111, under Step 8)
docker exec "$SPARK_MASTER_CONTAINER" \
    /spark/bin/spark-submit \
        --master          "$SPARK_MASTER" \
        --driver-memory   "$SPARK_DRIVER_MEMORY" \
        --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
        --conf "spark.sql.autoBroadcastJoinThreshold=$((100 * 1024 * 1024))" \
        --conf "spark.sql.shuffle.partitions=200" \
        "$PREPROCESS_SCRIPT"
# docker exec "$WORKSPACE_CONTAINER" \
#     spark-submit \
#         --master          "$SPARK_MASTER" \
#         --driver-memory   "$SPARK_DRIVER_MEMORY" \
#         --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
#         --conf "spark.sql.autoBroadcastJoinThreshold=$((100 * 1024 * 1024))" \
#         --conf "spark.sql.shuffle.partitions=200" \
#         --conf "spark.ui.port=4040" \
#         --conf "spark.driver.host=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $WORKSPACE_CONTAINER)" \
#         "$PREPROCESS_SCRIPT"

JOB_EXIT=$?

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

# ── Step 9: Result summary ────────────────────────────────────────────────────
if [ $JOB_EXIT -eq 0 ]; then
    echo ""
    success "Preprocessing job completed successfully!"
    echo ""
    info "Preprocessed output in HDFS:"
    docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls /user/project/data/preprocessed/
    echo ""
    echo -e "${GREEN}${BOLD}✅ All done. Parquet files written to HDFS:${RESET}"
    echo -e "   ${CYAN}hdfs://namenode:9000/user/project/data/preprocessed/${RESET}"
    echo ""
    echo -e "   Spark App UI  → ${CYAN}http://localhost:4040${RESET}"
    echo -e "   Spark Master  → ${CYAN}http://localhost:8080${RESET}"
    echo -e "   HDFS Web UI   → ${CYAN}http://localhost:9870${RESET}"
else
    echo ""
    error "Preprocessing job FAILED (exit code $JOB_EXIT) — check spark-submit logs above"
fi

exit $JOB_EXIT
