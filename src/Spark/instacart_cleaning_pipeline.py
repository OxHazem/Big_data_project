"""
instacart_cleaning_pipeline.py

Standalone PySpark job that reproduces the cleaning logic from
InstaCart_Processing.ipynb and writes 6 Hive-ready parquet datasets
to HDFS:/user/project/data/preprocessed/

Datasets produced:
  - aisles
  - departments
  - products
  - orders
  - order_products_prior
  - order_products_train

Run via spark-submit (see run_preprocessing.sh).
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
HDFS_NAMENODE = "hdfs://hadoop-namenode:9000"
RAW_DIR = f"{HDFS_NAMENODE}/user/project/data/raw"
PREPROCESSED_DIR = f"{HDFS_NAMENODE}/user/project/data/preprocessed"

RAW_FILES = {
    "aisles": f"{RAW_DIR}/aisles.csv",
    "departments": f"{RAW_DIR}/departments.csv",
    "products": f"{RAW_DIR}/products.csv",
    "orders": f"{RAW_DIR}/orders.csv",
    "order_products_prior": f"{RAW_DIR}/order_products__prior.csv",
    "order_products_train": f"{RAW_DIR}/order_products__train.csv",
}


def log(msg):
    print(f"[instacart_cleaning] {msg}", flush=True)


def null_report(df, name):
    """Same helper as the notebook: counts true nulls, empty strings,
    'NULL'/'None' string literals, and NaN per column.

    isnan() is restricted to numeric columns here (unlike the notebook,
    which applies it to every column) because newer Spark versions raise
    an AnalysisException when isnan() is called on a string column.
    """
    log(f"Null report -> {name}")
    numeric_types = ("double", "float", "int", "bigint", "smallint", "tinyint")
    dtypes = dict(df.dtypes)

    def null_condition(c):
        base = F.col(c).isNull() | (F.col(c) == '') | (F.col(c) == 'NULL') | (F.col(c) == 'None')
        if dtypes[c] in numeric_types:
            base = base | F.isnan(F.col(c))
        return base

    df.select([
        F.count(F.when(null_condition(c), c)).alias(c)
        for c in df.columns
    ]).show()


def read_raw(spark):
    log("Reading raw CSVs from HDFS ...")
    dfs = {
        name: spark.read.csv(path, header=True, inferSchema=True)
        for name, path in RAW_FILES.items()
    }
    for name, df in dfs.items():
        log(f"  {name}: {df.count()} rows, {len(df.columns)} cols")
    return dfs


def clean_products(products):
    """
    Notebook fixes applied:
      1. Drop the corrupt row (product_id == 6816) whose aisle_id /
         department_id are not numeric.
      2. Cast aisle_id / department_id from string -> int.
      3. Strip stray quotes / whitespace from product_name.
    """
    log("Cleaning products ...")

    products = products.filter(~products.product_id.isin(6816))

    cast_cols = []
    for c in products.columns:
        if c in ("department_id", "aisle_id"):
            cast_cols.append(products[c].cast(IntegerType()).alias(c))
        else:
            cast_cols.append(products[c])
    products = products.select(cast_cols)

    products = products.withColumn(
        "product_name",
        F.trim(F.regexp_replace("product_name", '"', ""))
    )

    products = products.dropDuplicates()
    return products


def clean_aisles(aisles):
    """Lowercase / trim / strip quotes on the aisle name."""
    log("Cleaning aisles ...")
    aisles = aisles.withColumn(
        "aisle",
        F.lower(F.trim(F.regexp_replace("aisle", '"', "")))
    )
    return aisles.dropDuplicates()


def clean_departments(departments):
    """Lowercase / trim / strip quotes on the department name."""
    log("Cleaning departments ...")
    departments = departments.withColumn(
        "department",
        F.lower(F.trim(F.regexp_replace("department", '"', "")))
    )
    return departments.dropDuplicates()


def clean_orders(orders):
    """Impute nulls in days_since_prior_order with 0 (first order)."""
    log("Cleaning orders ...")
    orders = orders.withColumn(
        "days_since_prior_order",
        F.when(orders["days_since_prior_order"].isNull(), 0)
        .otherwise(orders["days_since_prior_order"])
    )
    return orders.dropDuplicates()


def clean_order_products(df, name):
    """
    No column-level cleaning was applied to order_products_prior /
    order_products_train in the notebook. As a safety net for a
    production pipeline we drop exact duplicate rows only.
    """
    log(f"Cleaning {name} (dedupe only) ...")
    return df.dropDuplicates()


def validate_referential_integrity(products, aisles, departments, orders,
                                    order_products_prior, order_products_train):
    """Reproduce the notebook's sanity checks and log results (does not
    drop anything - purely informational, matching notebook behaviour)."""
    log("Validating referential integrity (informational only) ...")

    invalid_aisles = products.join(aisles, "aisle_id", "left_anti").count()
    invalid_departments = products.join(departments, "department_id", "left_anti").count()
    log(f"  products with unknown aisle_id: {invalid_aisles}")
    log(f"  products with unknown department_id: {invalid_departments}")

    missing_in_prior = orders.join(order_products_prior, "order_id", "left_anti").count()
    missing_in_train = orders.join(order_products_train, "order_id", "left_anti").count()
    log(f"  orders missing from order_products_prior: {missing_in_prior}")
    log(f"  orders missing from order_products_train: {missing_in_train}")


def write_parquet(df, name):
    out_path = f"{PREPROCESSED_DIR}/{name}"
    log(f"Writing {name} -> {out_path}")
    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("InstaCartCleaningPipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = read_raw(spark)

        for name, df in raw.items():
            null_report(df, name)

        products_clean = clean_products(raw["products"])
        aisles_clean = clean_aisles(raw["aisles"])
        departments_clean = clean_departments(raw["departments"])
        orders_clean = clean_orders(raw["orders"])
        order_products_prior_clean = clean_order_products(
            raw["order_products_prior"], "order_products_prior"
        )
        order_products_train_clean = clean_order_products(
            raw["order_products_train"], "order_products_train"
        )

        validate_referential_integrity(
            products_clean,
            aisles_clean,
            departments_clean,
            orders_clean,
            order_products_prior_clean,
            order_products_train_clean,
        )

        cleaned = {
            "aisles": aisles_clean,
            "departments": departments_clean,
            "products": products_clean,
            "orders": orders_clean,
            "order_products_prior": order_products_prior_clean,
            "order_products_train": order_products_train_clean,
        }

        for name, df in cleaned.items():
            write_parquet(df, name)

        log("All 6 cleaned datasets written successfully to "
            f"{PREPROCESSED_DIR}")

    except Exception as exc:
        log(f"ERROR: pipeline failed -> {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
