"""
=============================================================================
PySpark Preprocessing Pipeline — Instacart Market Basket Dataset
=============================================================================
Source: /user/project/data/raw/  (HDFS)
Output: /user/project/data/preprocessed/  (HDFS)

Addresses every issue catalogued in report/eda_report.md:

CRITICAL FIXES:
  [1] days_since_prior_order: fill nulls (first orders) with 0 + is_first_order flag
  [2] days_since_prior_order: add days_capped flag for hard ceiling at 30.0
  [3] products.csv: use proper CSV quoting to handle comma-embedded product names
  [4] "missing"/"other" placeholder categories → unified "unknown" sentinel class

IMPORTANT FIXES:
  [5] order_dow: documented as nominal categorical (0=Sunday assumption)
  [6] same_day_reorder flag where days_since_prior_order == 0
  [7] large_basket flag for orders with >50 products (prior set)
  [8] Cold-start annotation for products absent from train labels

SCHEMA:
  - All 6 tables use explicit StructType schemas (no inferSchema drift)

INFRASTRUCTURE:
  - Broadcast joins for small dimension tables (aisles, departments, products)
  - Prior set partitioned by user_id for user-level aggregations
  - Outputs written as Parquet (columnar, splittable, efficient for Spark)
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, FloatType, ShortType
)

# =============================================================================
# 1. SparkSession — connect to the cluster defined in docker-compose.yml
# =============================================================================


spark = (
    SparkSession.builder
    .appName("Instacart-Preprocessing")
    # REMOVE OR COMMENT OUT THIS LINE:
    # .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
# spark = (
#     SparkSession.builder
#     .appName("Instacart-Preprocessing")
#     # local[*] uses all available cores inside the workspace container.
#     # The bde2020 spark-master runs Spark 3.2.1 while this image uses
#     # Spark 3.5.0 — cross-version registration is not supported.
#     # HDFS is still accessed via hdfs://namenode:9000 below.
#     .master("local[*]")
#     # Point Spark at the HDFS NameNode (hadoop-namenode:9000)
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
#     # Broadcast threshold — tables < 100 MB will be auto-broadcast
#     .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
#     # Shuffle partitions tuned for ~33 M rows
#     .config("spark.sql.shuffle.partitions", "200")
#     .getOrCreate()
# )

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("  Instacart PySpark Preprocessing Pipeline — Starting")
print("=" * 70)

# =============================================================================
# 2. Explicit Schemas (Issue: inferSchema may silently misread types)
# =============================================================================

schema_aisles = StructType([
    StructField("aisle_id",   IntegerType(), nullable=False),
    StructField("aisle",      StringType(),  nullable=False),
])

schema_departments = StructType([
    StructField("department_id", IntegerType(), nullable=False),
    StructField("department",    StringType(),  nullable=False),
])

schema_products = StructType([
    StructField("product_id",    IntegerType(), nullable=False),
    StructField("product_name",  StringType(),  nullable=False),
    StructField("aisle_id",      IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=False),
])

schema_orders = StructType([
    StructField("order_id",                IntegerType(), nullable=False),
    StructField("user_id",                 IntegerType(), nullable=False),
    StructField("eval_set",                StringType(),  nullable=False),
    StructField("order_number",            IntegerType(), nullable=False),
    StructField("order_dow",               ShortType(),   nullable=False),
    StructField("order_hour_of_day",       ShortType(),   nullable=False),
    # Nullable: first order for every user has no prior order to measure from
    StructField("days_since_prior_order",  FloatType(),   nullable=True),
])

schema_order_products = StructType([
    StructField("order_id",          IntegerType(), nullable=False),
    StructField("product_id",        IntegerType(), nullable=False),
    StructField("add_to_cart_order", IntegerType(), nullable=False),
    StructField("reordered",         ShortType(),   nullable=False),
])

# =============================================================================
# 3. Read raw CSVs from HDFS
#    - quote='"'  → handles RFC-4180 double-quoted product names with commas
#      (Issue [3]: naive splitting would break product_name / shift columns)
#    - escape='"' → handles "" escaped quotes within strings
#    - multiLine is intentionally False (no embedded newlines expected)
# =============================================================================

HDFS_RAW  = "hdfs://namenode:9000/user/project/data/raw"
HDFS_OUT  = "hdfs://namenode:9000/user/project/data/preprocessed"

CSV_OPTS = dict(header="true", quote='"', escape='"')

print("\n[1/8] Reading dimension tables from HDFS...")

df_aisles = spark.read.csv(
    f"{HDFS_RAW}/aisles.csv",
    schema=schema_aisles,
    **CSV_OPTS
)

df_departments = spark.read.csv(
    f"{HDFS_RAW}/departments.csv",
    schema=schema_departments,
    **CSV_OPTS
)

df_products_raw = spark.read.csv(
    f"{HDFS_RAW}/products.csv",
    schema=schema_products,
    **CSV_OPTS
)

print("[2/8] Reading fact tables from HDFS...")

df_orders_raw = spark.read.csv(
    f"{HDFS_RAW}/orders.csv",
    schema=schema_orders,
    **CSV_OPTS
)

df_prior_raw = spark.read.csv(
    f"{HDFS_RAW}/order_products__prior.csv",
    schema=schema_order_products,
    **CSV_OPTS
)

df_train_raw = spark.read.csv(
    f"{HDFS_RAW}/order_products__train.csv",
    schema=schema_order_products,
    **CSV_OPTS
)

# =============================================================================
# 4. Preprocess: orders.csv
# =============================================================================
print("[3/8] Preprocessing orders.csv...")

df_orders = (
    df_orders_raw
    # ------------------------------------------------------------------
    # Issue [1]: days_since_prior_order is structurally null for every
    #            user's first order (order_number=1). Fill with 0 as the
    #            sentinel — model features will also use is_first_order.
    # ------------------------------------------------------------------
    .withColumn(
        "is_first_order",
        F.col("order_number") == 1
    )
    .withColumn(
        "days_since_prior_order",
        F.when(F.col("days_since_prior_order").isNull(), F.lit(0.0))
         .otherwise(F.col("days_since_prior_order"))
    )
    # ------------------------------------------------------------------
    # Issue [2]: The value 30.0 is a hard ceiling — any gap ≥ 30 days is
    #            recorded as exactly 30. Add a binary flag to preserve this
    #            information so models are not misled by the truncation.
    # ------------------------------------------------------------------
    .withColumn(
        "days_capped",
        F.col("days_since_prior_order") == 30.0
    )
    # ------------------------------------------------------------------
    # Issue [6]: Same-day reorders (value = 0) may be legitimate but are
    #            worth flagging for downstream investigation / modelling.
    # ------------------------------------------------------------------
    .withColumn(
        "same_day_reorder",
        (F.col("days_since_prior_order") == 0.0) & (~F.col("is_first_order"))
    )
    # ------------------------------------------------------------------
    # Issue [5]: order_dow encoding is undocumented. We treat it as a
    #            nominal categorical (not ordinal) by casting to string so
    #            downstream one-hot encoding does not imply ordinality.
    #            Document the most-likely mapping: 0=Sunday, 1=Monday, …
    # ------------------------------------------------------------------
    .withColumn(
        "order_dow_label",
        F.when(F.col("order_dow") == 0, "Sunday")
         .when(F.col("order_dow") == 1, "Monday")
         .when(F.col("order_dow") == 2, "Tuesday")
         .when(F.col("order_dow") == 3, "Wednesday")
         .when(F.col("order_dow") == 4, "Thursday")
         .when(F.col("order_dow") == 5, "Friday")
         .when(F.col("order_dow") == 6, "Saturday")
         .otherwise("Unknown")
    )
    # ------------------------------------------------------------------
    # Convenience: bin order_hour_of_day into named time-of-day buckets
    # (EDA section 4.6 — unimodal distribution, clean column)
    # ------------------------------------------------------------------
    .withColumn(
        "hour_bin",
        F.when(F.col("order_hour_of_day").between(0,  5),  "late_night")
         .when(F.col("order_hour_of_day").between(6,  9),  "morning")
         .when(F.col("order_hour_of_day").between(10, 16), "daytime")
         .when(F.col("order_hour_of_day").between(17, 23), "evening")
         .otherwise("unknown")
    )
)

# ------------------------------------------------------------------
# Validation: null counts should now be 0 for days_since_prior_order
# ------------------------------------------------------------------
null_check = df_orders.filter(F.col("days_since_prior_order").isNull()).count()
assert null_check == 0, f"[FAIL] days_since_prior_order still has {null_check} nulls!"
print(f"  ✅ days_since_prior_order: 0 nulls after fill")

# =============================================================================
# 5. Preprocess: products.csv — placeholder category handling
# =============================================================================
print("[4/8] Preprocessing products.csv...")

# Issue [4]: aisle_id=100 → "missing", aisle_id=6 → "other"
#            department_id=21 → "missing", department_id=2 → "other"
#            These are given a unified sentinel label "unknown" in the
#            dimension tables so that joins propagate the label cleanly.

df_aisles_clean = df_aisles.withColumn(
    "aisle",
    F.when(F.col("aisle").isin("missing", "other"), "unknown")
     .otherwise(F.col("aisle"))
)

df_departments_clean = df_departments.withColumn(
    "department",
    F.when(F.col("department").isin("missing", "other"), "unknown")
     .otherwise(F.col("department"))
)

# Issue [10] (informational): flag the 3 products never ordered in the dataset.
COLD_PRODUCT_IDS = [3630, 46625, 7045]

# Broadcast the small dimension tables — fits comfortably in executor memory
df_products = (
    df_products_raw
    .join(F.broadcast(df_aisles_clean),      on="aisle_id",      how="left")
    .join(F.broadcast(df_departments_clean), on="department_id",  how="left")
    # Convenience boolean flags mirroring the unknown sentinel
    .withColumn(
        "aisle_is_unknown",
        F.col("aisle") == "unknown"
    )
    .withColumn(
        "department_is_unknown",
        F.col("department") == "unknown"
    )
    # Informational: 3 products that were never ordered in any basket
    .withColumn(
        "is_catalog_only_product",
        F.col("product_id").isin(COLD_PRODUCT_IDS)
    )
)

# =============================================================================
# 6. Preprocess: order_products__prior.csv
# =============================================================================
print("[5/8] Preprocessing order_products__prior.csv...")

# Issue [7]: flag large baskets (>50 products) as potential outliers.
# First compute basket sizes so we can join the flag back.
basket_sizes_prior = (
    df_prior_raw
    .groupBy("order_id")
    .agg(F.count("product_id").alias("basket_size"))
)

df_prior = (
    df_prior_raw
    .join(basket_sizes_prior, on="order_id", how="left")
    .withColumn(
        "is_large_basket",
        F.col("basket_size") > 50
    )
)

# Validate: no FK violations — all product_ids must exist in products
# (EDA confirmed 0 violations; we re-verify after preprocessing)
prior_fk_violations = (
    df_prior
    .join(F.broadcast(df_products_raw.select("product_id")),
          on="product_id", how="left_anti")
    .count()
)
assert prior_fk_violations == 0, \
    f"[FAIL] prior set has {prior_fk_violations} product_id FK violations!"
print(f"  ✅ order_products__prior: 0 FK violations")

# =============================================================================
# 7. Preprocess: order_products__train.csv
# =============================================================================
print("[6/8] Preprocessing order_products__train.csv...")

# Issue [8]: identify products in the catalog that have NO training label.
#            We annotate each product with a flag rather than dropping them,
#            preserving the ability to apply cold-start strategies downstream.
train_product_ids = df_train_raw.select("product_id").distinct()

df_products_with_cold_start = df_products.join(
    F.broadcast(train_product_ids.withColumn("has_train_label", F.lit(True))),
    on="product_id",
    how="left"
).withColumn(
    "has_train_label",
    F.coalesce(F.col("has_train_label"), F.lit(False))
).withColumn(
    # True = this product never appears as a training label → cold-start risk
    "is_cold_start_product",
    ~F.col("has_train_label")
)

basket_sizes_train = (
    df_train_raw
    .groupBy("order_id")
    .agg(F.count("product_id").alias("basket_size"))
)

df_train = (
    df_train_raw
    .join(basket_sizes_train, on="order_id", how="left")
    .withColumn(
        "is_large_basket",
        F.col("basket_size") > 50
    )
)

# Validate FK integrity for train
train_fk_violations = (
    df_train
    .join(F.broadcast(df_products_raw.select("product_id")),
          on="product_id", how="left_anti")
    .count()
)
assert train_fk_violations == 0, \
    f"[FAIL] train set has {train_fk_violations} product_id FK violations!"
print(f"  ✅ order_products__train: 0 FK violations")

# =============================================================================
# 8. Final null / schema validation summary
# =============================================================================
print("[7/8] Running final validation checks...")

def validate_no_nulls(df, name: str, columns: list):
    for col_name in columns:
        n = df.filter(F.col(col_name).isNull()).count()
        status = "✅" if n == 0 else f"❌ {n} nulls"
        print(f"  {status}  {name}.{col_name}")

validate_no_nulls(df_orders, "orders",
    ["order_id", "user_id", "eval_set", "order_number",
     "order_dow", "order_hour_of_day", "days_since_prior_order",
     "is_first_order", "days_capped", "same_day_reorder",
     "order_dow_label", "hour_bin"])

validate_no_nulls(df_products_with_cold_start, "products",
    ["product_id", "product_name", "aisle_id", "department_id",
     "aisle", "department", "has_train_label", "is_cold_start_product"])

validate_no_nulls(df_prior, "order_products__prior",
    ["order_id", "product_id", "add_to_cart_order", "reordered",
     "basket_size", "is_large_basket"])

validate_no_nulls(df_train, "order_products__train",
    ["order_id", "product_id", "add_to_cart_order", "reordered",
     "basket_size", "is_large_basket"])

# =============================================================================
# 9. Write preprocessed data to HDFS as Parquet
#
#    Rationale for Parquet:
#      - Columnar format: reads only required columns
#      - Splittable: Spark workers can read in parallel
#      - Preserves types: no silent re-parsing of strings
#      - Compression: snappy by default (~3–5× smaller than CSV)
#
#    Partitioning strategy:
#      - orders: partition by eval_set (prior / train / test) — makes
#        downstream "train only" or "prior only" reads ~3× faster
#      - order_products__prior: partition by user_id bucket; because
#        user_id has 206K unique values we bucket into 200 partitions
#        using repartition to keep file count manageable
#      - Products / dimension tables: no partitioning (small tables)
# =============================================================================
print("[8/8] Writing preprocessed Parquet files to HDFS...")

# ---- Dimension tables (small — write as single Parquet files) ---------------
(
    df_aisles_clean
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/aisles")
)
print("  ✅ aisles written")

(
    df_departments_clean
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/departments")
)
print("  ✅ departments written")

(
    df_products_with_cold_start
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/products")
)
print("  ✅ products written")

# ---- Fact table: orders (partitioned by eval_set) ---------------------------
(
    df_orders
    .write.mode("overwrite")
    .partitionBy("eval_set")
    .parquet(f"{HDFS_OUT}/orders")
)
print("  ✅ orders written (partitioned by eval_set)")

# ---- Fact table: order_products__prior (repartitioned by user_id) -----------
# Join with orders to get user_id, then repartition for user-level aggregations
df_prior_with_user = (
    df_prior
    .join(
        F.broadcast(df_orders.select("order_id", "user_id", "eval_set")),
        on="order_id",
        how="left"
    )
    # Repartition into 200 roughly equal buckets by user_id
    .repartition(200, F.col("user_id"))
)

(
    df_prior_with_user
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/order_products_prior")
)
print("  ✅ order_products__prior written (200 partitions by user_id)")

# ---- Fact table: order_products__train (small enough for ~10 partitions) ----
(
    df_train
    .repartition(10)
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/order_products_train")
)
print("  ✅ order_products__train written")

# =============================================================================
# 10. Summary report
# =============================================================================
print("\n" + "=" * 70)
print("  Preprocessing Complete — Summary")
print("=" * 70)
print(f"\n  Input  HDFS path : {HDFS_RAW}")
print(f"  Output HDFS path : {HDFS_OUT}")
print("""
  Files written (Parquet):
    preprocessed/aisles/
    preprocessed/departments/
    preprocessed/products/
    preprocessed/orders/             ← partitioned by eval_set
    preprocessed/order_products_prior/  ← 200 partitions by user_id
    preprocessed/order_products_train/

  Issues resolved:
    [✅ Critical-1] days_since_prior_order nulls filled with 0; is_first_order added
    [✅ Critical-2] days_capped flag added for hard-ceiling at 30.0
    [✅ Critical-3] products.csv parsed with RFC-4180 quoting (quote='"')
    [✅ Critical-4] "missing"/"other" categories unified to "unknown"
    [✅ Important-5] order_dow_label column added (0=Sunday assumption documented)
    [✅ Important-6] same_day_reorder flag added (days_since_prior_order == 0)
    [✅ Important-7] is_large_basket flag added for orders with >50 products
    [✅ Important-8] is_cold_start_product flag added for products absent from train
    [✅ Schema]      Explicit StructType schemas for all 6 source files
    [✅ FK Integrity] Re-validated after preprocessing — 0 violations
""")

spark.stop()
