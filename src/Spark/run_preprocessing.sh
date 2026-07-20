#!/bin/bash
set -euo pipefail

echo "========================================"
echo "🧹 Starting InstaCart Preprocessing Phase..."
echo "========================================"

# --- Config -------------------------------------------------------------
# Docker container that has spark-submit on its PATH and network access
# to hadoop-namenode. Adjust SPARK_CONTAINER to match your docker-compose
# service name (e.g. "spark-master", "spark-client", "hadoop-namenode" if
# Spark is co-located with HDFS).
SPARK_CONTAINER="spark-master"

# Local path (on the host) to the pipeline script
LOCAL_SCRIPT_DIR="/home/pop/Big_data_project/pyspark"
LOCAL_SCRIPT="$LOCAL_SCRIPT_DIR/instacart_cleaning_pipeline.py"

# Path to copy/run the script from inside the container
CONTAINER_SCRIPT_DIR="/tmp"
CONTAINER_SCRIPT="$CONTAINER_SCRIPT_DIR/instacart_cleaning_pipeline.py"

# Spark resource settings — tune to your cluster
SPARK_MASTER_URL="local[*]"
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="4g"

# --- 1. Sanity check: raw data must already be in HDFS -------------------
echo "[1/4] Checking that raw data exists in HDFS (/user/project/data/raw)..."
docker exec hadoop-namenode hdfs dfs -test -d /user/project/data/raw
if [ $? -ne 0 ]; then
    echo "❌ /user/project/data/raw not found. Run init_HDFS.sh first."
    exit 1
fi
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/project/data/preprocessed

# --- 2. Ship the pyspark script into the Spark container ------------------
echo "[2/4] Copying pipeline script into $SPARK_CONTAINER..."
docker cp "$LOCAL_SCRIPT" "$SPARK_CONTAINER":"$CONTAINER_SCRIPT"

# --- 3. Submit the Spark job ----------------------------------------------
echo "[3/4] Submitting Spark job (spark-submit)..."
docker exec "$SPARK_CONTAINER" spark-submit \
    --master "$SPARK_MASTER_URL" \
    --name "InstaCartCleaningPipeline" \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --conf spark.sql.catalogImplementation=hive \
    "$CONTAINER_SCRIPT"

# --- 4. Verify output ------------------------------------------------------
echo "[4/4] Verification: Listing cleaned datasets in HDFS..."
docker exec hadoop-namenode hdfs dfs -ls /user/project/data/preprocessed/

echo "========================================"
echo "✅ Preprocessing Phase Complete! 6 cleaned parquet datasets written to"
echo "   /user/project/data/preprocessed/"
echo "   (aisles, departments, products, orders,"
echo "    order_products_prior, order_products_train)"
echo "========================================"
