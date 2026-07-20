#!/bin/bash

echo "========================================"
echo "🚀 Starting HDFS Initialization Phase..."
echo "========================================"

# 1. Create the structured directories inside HDFS
echo "[1/3] Creating HDFS directory structure (/user/project/data/raw)..."
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/project/data/raw
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/project/data/preprocessed

echo "[2/3] Transferring and Uploading files to HDFS..."

# 2. Use the absolute path to your data folder to prevent directory errors
RAW_DATA_DIR="/home/pop/Big_data_project/data/raw"

for FILE_PATH in "$RAW_DATA_DIR"/*.csv; do
    # Extract just the filename from the full path
    FILE=$(basename "$FILE_PATH")
    
    echo "  -> Processing $FILE..."
    
    # Copy from the exact local path to the NameNode container's temp folder
    docker cp "$FILE_PATH" hadoop-namenode:/tmp/"$FILE"
    
    # Move the file from the container's temp folder into HDFS
    docker exec hadoop-namenode hdfs dfs -put -f /tmp/"$FILE" /user/project/data/raw/
    
    # Clean up the temp file inside the container to save space
    docker exec hadoop-namenode rm /tmp/"$FILE"
done

# 3. Verify the upload
echo "[3/3] Verification: Listing files in HDFS..."
docker exec hadoop-namenode hdfs dfs -ls /user/project/data/raw/

echo "========================================"
echo "✅ HDFS Phase Complete! All found CSVs are ready."
echo "========================================"