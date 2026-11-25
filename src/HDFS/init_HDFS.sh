#!/bin/bash

echo "========================================"
echo "🚀 Starting HDFS Initialization Phase..."
echo "========================================"

echo "[1/3] Creating HDFS directory structure (/user/project/data/raw)..."
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/project/data/raw
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/project/data/preprocessed

echo "[2/3] Transferring and Uploading files to HDFS..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RAW_DATA_DIR="$(cd "$PROJECT_ROOT/data/raw" && pwd)"

for FILE_PATH in "$RAW_DATA_DIR"/*.csv; do
    FILE=$(basename "$FILE_PATH")
    
    echo "  -> Processing $FILE..."
    
    docker cp "$FILE_PATH" hadoop-namenode:/tmp/"$FILE"
    
    docker exec hadoop-namenode hdfs dfs -put -f /tmp/"$FILE" /user/project/data/raw/
    
    docker exec hadoop-namenode rm /tmp/"$FILE"
done

echo "[3/3] Verification: Listing files in HDFS..."
docker exec hadoop-namenode hdfs dfs -ls /user/project/data/raw/

echo "========================================"
echo "✅ HDFS Phase Complete! All found CSVs are ready."
echo "========================================"
