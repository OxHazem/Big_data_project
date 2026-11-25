#!/bin/bash

set -euo pipefail

CYAN='\033[0;36m'
GREEN='\033[0;32m'
RESET='\033[0m'

echo -e "${CYAN}========================================${RESET}"
echo -e "${CYAN}📥 Downloading Preprocessed Data from HDFS...${RESET}"
echo -e "${CYAN}========================================${RESET}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOCAL_DEST="$PROJECT_ROOT/data/preprocessed"

echo "[1/4] Preparing local directory..."
mkdir -p "$LOCAL_DEST"
echo "  -> Local path ready: $LOCAL_DEST"

echo "[2/4] Extracting data from HDFS to NameNode temp storage..."
docker exec hadoop-namenode rm -rf /tmp/preprocessed_export
docker exec hadoop-namenode hdfs dfs -get /user/project/data/preprocessed /tmp/preprocessed_export

echo "[3/4] Transferring files from container to host machine..."
docker cp hadoop-namenode:/tmp/preprocessed_export/. "$LOCAL_DEST/"

echo "[4/4] Cleaning up NameNode temp storage..."
docker exec hadoop-namenode rm -rf /tmp/preprocessed_export

echo -e "${GREEN}========================================${RESET}"
echo -e "${GREEN}✅ Export Complete! Files are ready in data/preprocessed/${RESET}"
echo -e "${GREEN}========================================${RESET}"
