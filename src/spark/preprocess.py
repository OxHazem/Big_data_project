from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, FloatType, ShortType
)


spark = (
    SparkSession.builder
    .appName("Instacart-Preprocessing")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("  Instacart PySpark Preprocessing Pipeline — Starting")
print("=" * 70)

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
    StructField("days_since_prior_order",  FloatType(),   nullable=True),
])

schema_order_products = StructType([
    StructField("order_id",          IntegerType(), nullable=False),
    StructField("product_id",        IntegerType(), nullable=False),
    StructField("add_to_cart_order", IntegerType(), nullable=False),
    StructField("reordered",         ShortType(),   nullable=False),
])

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

print("[3/8] Preprocessing orders.csv...")

df_orders = (
    df_orders_raw
    .withColumn(
        "is_first_order",
        F.col("order_number") == 1
    )
    .withColumn(
        "days_since_prior_order",
        F.when(F.col("days_since_prior_order").isNull(), F.lit(0.0))
         .otherwise(F.col("days_since_prior_order"))
    )
    .withColumn(
        "days_capped",
        F.col("days_since_prior_order") == 30.0
    )
    .withColumn(
        "same_day_reorder",
        (F.col("days_since_prior_order") == 0.0) & (~F.col("is_first_order"))
    )
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
    .withColumn(
        "hour_bin",
        F.when(F.col("order_hour_of_day").between(0,  5),  "late_night")
         .when(F.col("order_hour_of_day").between(6,  9),  "morning")
         .when(F.col("order_hour_of_day").between(10, 16), "daytime")
         .when(F.col("order_hour_of_day").between(17, 23), "evening")
         .otherwise("unknown")
    )
)

null_check = df_orders.filter(F.col("days_since_prior_order").isNull()).count()
assert null_check == 0, f"[FAIL] days_since_prior_order still has {null_check} nulls!"
print(f"  ✅ days_since_prior_order: 0 nulls after fill")

print("[4/8] Preprocessing products.csv...")

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

COLD_PRODUCT_IDS = [3630, 46625, 7045]

df_products = (
    df_products_raw
    .join(F.broadcast(df_aisles_clean),      on="aisle_id",      how="left")
    .join(F.broadcast(df_departments_clean), on="department_id",  how="left")
    .withColumn(
        "aisle_is_unknown",
        F.col("aisle") == "unknown"
    )
    .withColumn(
        "department_is_unknown",
        F.col("department") == "unknown"
    )
    .withColumn(
        "is_catalog_only_product",
        F.col("product_id").isin(COLD_PRODUCT_IDS)
    )
)

print("[5/8] Preprocessing order_products__prior.csv...")

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

prior_fk_violations = (
    df_prior
    .join(F.broadcast(df_products_raw.select("product_id")),
          on="product_id", how="left_anti")
    .count()
)
assert prior_fk_violations == 0, \
    f"[FAIL] prior set has {prior_fk_violations} product_id FK violations!"
print(f"  ✅ order_products__prior: 0 FK violations")

print("[6/8] Preprocessing order_products__train.csv...")

train_product_ids = df_train_raw.select("product_id").distinct()

df_products_with_cold_start = df_products.join(
    F.broadcast(train_product_ids.withColumn("has_train_label", F.lit(True))),
    on="product_id",
    how="left"
).withColumn(
    "has_train_label",
    F.coalesce(F.col("has_train_label"), F.lit(False))
).withColumn(
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

train_fk_violations = (
    df_train
    .join(F.broadcast(df_products_raw.select("product_id")),
          on="product_id", how="left_anti")
    .count()
)
assert train_fk_violations == 0, \
    f"[FAIL] train set has {train_fk_violations} product_id FK violations!"
print(f"  ✅ order_products__train: 0 FK violations")

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

print("[8/8] Writing preprocessed Parquet files to HDFS...")

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

(
    df_orders
    .write.mode("overwrite")
    .partitionBy("eval_set")
    .parquet(f"{HDFS_OUT}/orders")
)
print("  ✅ orders written (partitioned by eval_set)")

df_prior_with_user = (
    df_prior
    .join(
        F.broadcast(df_orders.select("order_id", "user_id", "eval_set")),
        on="order_id",
        how="left"
    )
    .repartition(200, F.col("user_id"))
)

(
    df_prior_with_user
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/order_products_prior")
)
print("  ✅ order_products__prior written (200 partitions by user_id)")

(
    df_train
    .repartition(10)
    .write.mode("overwrite")
    .parquet(f"{HDFS_OUT}/order_products_train")
)
print("  ✅ order_products__train written")

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
