# 📊 Exploratory Data Analysis Report
## Instacart Market Basket Dataset — `data/raw`
> **Generated**: 2026-07-20 | **Purpose**: Data quality assessment before PySpark preprocessing

---

## 1. Dataset Overview

This is the **Instacart Market Basket Analysis** dataset — a normalized relational dataset describing the online grocery ordering behavior of ~206,000 users across ~3.4 million orders and ~33.8 million order-product records.

The dataset consists of **6 CSV files** organized in a star-schema structure:

| File | Size (on disk) | Rows (excl. header) | Role |
|---|---|---|---|
| `aisles.csv` | 2.6 KB | 134 | Dimension — product location |
| `departments.csv` | 270 B | 21 | Dimension — product category |
| `products.csv` | 2.2 MB | 49,688 | Dimension — product catalog |
| `orders.csv` | 109 MB | 3,421,083 | Fact — order metadata |
| `order_products__prior.csv` | 578 MB | 32,434,489 | Fact — historical basket contents |
| `order_products__train.csv` | 24.7 MB | 1,384,617 | Fact — training basket contents |

**Total fact rows**: ~33.8 million product-order records

---

## 2. Schema & Column Details

### 2.1 `aisles.csv`
| Column | Type | Description |
|---|---|---|
| `aisle_id` | Integer | Primary key (1–134) |
| `aisle` | String | Aisle label |

### 2.2 `departments.csv`
| Column | Type | Description |
|---|---|---|
| `department_id` | Integer | Primary key (1–21) |
| `department` | String | Department label |

### 2.3 `products.csv`
| Column | Type | Description |
|---|---|---|
| `product_id` | Integer | Primary key (1–49,688) |
| `product_name` | String | Product name (may contain commas) |
| `aisle_id` | Integer | Foreign key → `aisles.aisle_id` |
| `department_id` | Integer | Foreign key → `departments.department_id` |

### 2.4 `orders.csv`
| Column | Type | Description |
|---|---|---|
| `order_id` | Integer | Primary key |
| `user_id` | Integer | Foreign key — user identifier |
| `eval_set` | String | Partition label: `prior`, `train`, `test` |
| `order_number` | Integer | Sequential order count per user (starts at 1) |
| `order_dow` | Integer | Day of week (0–6, encoding unknown) |
| `order_hour_of_day` | Integer | Hour of order (0–23) |
| `days_since_prior_order` | Float | Days since the user's previous order (null for first order) |

### 2.5 `order_products__prior.csv` & `order_products__train.csv`
| Column | Type | Description |
|---|---|---|
| `order_id` | Integer | Foreign key → `orders.order_id` |
| `product_id` | Integer | Foreign key → `products.product_id` |
| `add_to_cart_order` | Integer | Position in which product was added to cart |
| `reordered` | Binary (0/1) | Whether user previously ordered this product |

---

## 3. Data Split & Partitioning

The `eval_set` column in `orders.csv` divides orders into three mutually exclusive partitions:

| eval_set | Orders | Users | Notes |
|---|---|---|---|
| `prior` | 3,214,874 (93.97%) | 206,209 | All historical orders per user |
| `train` | 131,209 (3.83%) | 131,209 | One most-recent order per user (labeled) |
| `test` | 75,000 (2.19%) | 75,000 | One most-recent order per user (unlabeled) |

**Key structural facts:**
- Every user has **exactly 1** `train` order and **exactly 1** `test` order (confirmed, no user has more than 1 in either set)
- **131,209 users** have both a `train` and prior orders; **75,000 users** have only `test` and prior orders
- The **`test`** orders have **no entries** in `order_products__prior.csv` or `order_products__train.csv` — their basket contents are the prediction target

---

## 4. `orders.csv` — Detailed Analysis

### 4.1 Null / Missing Values

| Column | Null Count | % Null | Notes |
|---|---|---|---|
| `order_id` | 0 | 0% | ✅ Complete |
| `user_id` | 0 | 0% | ✅ Complete |
| `eval_set` | 0 | 0% | ✅ Complete |
| `order_number` | 0 | 0% | ✅ Complete |
| `order_dow` | 0 | 0% | ✅ Complete |
| `order_hour_of_day` | 0 | 0% | ✅ Complete |
| `days_since_prior_order` | **206,209** | **6.03%** | ⚠️ **Structurally expected nulls** |

> [!IMPORTANT]
> The 206,209 nulls in `days_since_prior_order` are **not random** — they correspond exactly to each user's **first order** (`order_number = 1`). Validation confirms: zero first-orders have a non-null value, and zero non-first-orders have a null value. This is **by design**, not data corruption. However, the column must be handled in PySpark (e.g., fill with 0 or a sentinel value before feature engineering).

### 4.2 Duplicate Check
- **Duplicate `order_id`**: 0 ✅
- **Duplicate `user_id` per eval_set**: Each user appears exactly once in `train` and once in `test` ✅

### 4.3 Order Volume per User

| Metric | Value |
|---|---|
| Unique users | 206,209 |
| Total orders | 3,421,083 |
| Min orders per user | 4 |
| Max orders per user | 100 |
| Avg orders per user | ~16.6 |

> [!NOTE]
> The minimum of **4 orders per user** indicates this dataset is **filtered** — users with very few orders were excluded. There is likely a threshold applied upstream. This means the dataset skews toward *engaged* users, potentially over-representing heavy shoppers.

### 4.4 `order_number` Continuity
- Users where `max(order_number) ≠ count(orders)`: **0** ✅
- `order_number` is a clean, consecutive sequence from 1 to N per user. No gaps.

### 4.5 `order_dow` Distribution (Day of Week)

| DOW Code | Order Count | % Share |
|---|---|---|
| 0 | 600,905 | 17.6% |
| 1 | 587,478 | 17.2% |
| 2 | 467,260 | 13.7% |
| 3 | 436,972 | 12.8% |
| 4 | 426,339 | 12.5% |
| 5 | 453,368 | 13.2% |
| 6 | 448,761 | 13.1% |

> [!WARNING]
> **The `order_dow` encoding is ambiguous.** Day 0 and Day 1 together account for ~35% of all orders, strongly suggesting they represent **Sunday and Saturday** (weekend shopping spike). However, the dataset provides **no documentation** mapping numeric codes to day names. This ambiguity must be addressed — treat the column as a **nominal categorical feature** (one-hot encode) rather than a continuous ordinal, unless the day-name mapping is confirmed from external sources.

### 4.6 `order_hour_of_day` Distribution

| Range | Pattern |
|---|---|
| 0–5 (late night) | Low volume (~63K orders total) |
| 6–9 (morning ramp) | Rapidly increasing (~368K) |
| 10–16 (peak daytime) | Highest density (~1.9M) |
| 17–23 (evening decline) | Gradually decreasing (~796K) |

> [!NOTE]
> The distribution is unimodal and right-skewed, peaking around **10:00–15:00**. No anomalous hours detected (values stay within 0–23). This column is clean but will benefit from **binning** (e.g., morning/afternoon/evening/night) during feature engineering.

### 4.7 `days_since_prior_order` Analysis (Non-Null Values Only)

| Metric | Value |
|---|---|
| Count (non-null) | 3,214,874 |
| Min | 0.0 |
| Max | 30.0 |
| Mean | ~11.1 days |
| Mode / spike at 30 | 369,323 orders (11.5%) |
| Value = 0 | 67,755 orders (2.1%) |

> [!CAUTION]
> **The value is capped at 30.0.** With 369,323 orders at exactly `30.0` (more than 3× any other single value), this represents a **hard ceiling** — any gap ≥ 30 days is recorded as 30. This is a known data truncation artifact. Treating 30.0 as a continuous value will **underestimate** true order gaps for a significant portion of orders. During preprocessing, consider treating `days_since_prior_order = 30` as a censored value or a separate indicator flag.

> [!WARNING]
> **`days_since_prior_order = 0.0`** appears for 67,755 orders — meaning the user placed another order on the **same day**. This is possible but worth investigating whether these are legitimate same-day orders or data entry artifacts. They should not be dropped without investigation, but may need to be flagged.

---

## 5. `products.csv` — Detailed Analysis

### 5.1 Null / Missing Values
| Column | Null Count | Notes |
|---|---|---|
| `product_id` | 0 | ✅ Complete |
| `product_name` | 0 | ✅ Complete |
| `aisle_id` | 0 | ✅ Complete |
| `department_id` | 0 | ✅ Complete |

### 5.2 Duplicate Check
- **Duplicate `product_id`**: 0 ✅
- **Duplicate `product_name`**: 0 ✅

### 5.3 Categorical Ranges
| Column | Min | Max | Notes |
|---|---|---|---|
| `aisle_id` | 1 | 134 | Matches `aisles.csv` range ✅ |
| `department_id` | 1 | 21 | Matches `departments.csv` range ✅ |

### 5.4 Referential Integrity (FK to dimension tables)
- `aisle_id` values not in `aisles.csv`: **0** ✅
- `department_id` values not in `departments.csv`: **0** ✅
- All 134 aisles are used; all 21 departments are used ✅

### 5.5 "Missing" Category Pollution

> [!CAUTION]
> Both the `aisles` and `departments` dimension tables contain placeholder/unknown categories:
> - `aisle_id = 100` → aisle label: **"missing"** — affects **1,258 products** (2.53%)
> - `department_id = 21` → department label: **"missing"** — affects **1,258 products** (2.53%)
> - `aisle_id = 6` → aisle label: **"other"** — affects **548 products** (1.10%)
> - `department_id = 2` → department label: **"other"** — affects **548 products** (1.10%)
>
> The 1,258 "missing" products and 548 "other" products share the same IDs across aisle and department assignments — they are co-classified. These products have **no meaningful categorical label** and will skew any category-based aggregations. They should be filtered, imputed, or treated as a separate "unknown" class during preprocessing.

### 5.6 Product Name Parsing Issue

> [!WARNING]
> **Many product names contain embedded commas** and are **quoted with double-quotes** (RFC 4180 CSV quoting). For example:
> - `"Three Cheese Ziti, Marinara with Meatballs"`
> - `"School Glue, Washable, No Run"`
>
> A naive `split(",")` in PySpark will **break these fields** into multiple columns, misaligning `aisle_id` and `department_id`. You must use PySpark's CSV reader with `quote='"'` option (which is the default) and **avoid manual string splitting**. Always use `spark.read.csv(..., header=True, inferSchema=True, quote='"', escape='"')` or equivalent.

### 5.7 Cold Products (Never Ordered)
- Products in `products.csv` with **no orders** in either prior or train: **3 products** (IDs: `3630`, `46625`, `7045`)
- These are catalog entries that were never purchased in the dataset window. They are harmless but contribute noise if used in collaborative filtering models.

---

## 6. `order_products__prior.csv` — Detailed Analysis

### 6.1 Null / Missing Values
| Column | Null Count | Notes |
|---|---|---|
| `order_id` | 0 | ✅ Complete |
| `product_id` | 0 | ✅ Complete |
| `add_to_cart_order` | 0 | ✅ Complete |
| `reordered` | 0 | ✅ Complete |

### 6.2 Scale
- **32,434,489 rows** covering **3,214,874 prior orders**
- This is the primary analytical table — ~96% of all order-product records

### 6.3 `reordered` Distribution
| Value | Count | % |
|---|---|---|
| 0 (not reordered) | 13,307,953 | 41.0% |
| 1 (reordered) | 19,126,536 | 59.0% |

> [!NOTE]
> A **59% reorder rate** in the prior set indicates strong habit-based purchasing behavior. This column is clean (binary, no nulls), but note: **a product's first-ever appearance for a user is always reordered=0**, even if it was later re-purchased — "reordered" refers to whether the user had ordered this exact product in *any earlier order* (not the immediately prior one).

### 6.4 `add_to_cart_order` Statistics
| Metric | Value |
|---|---|
| Min | 1 |
| Max | 145 |
| Mean | ~8.35 |

> [!WARNING]
> The maximum `add_to_cart_order` is **145** in the prior set (vs. 80 in train). This represents extremely large baskets. While technically valid, these extreme outliers (orders with 100+ items) could distort feature distributions and model training. Consider investigating the frequency of orders with >50 products and whether they represent B2B or bulk orders vs. individual consumers.

### 6.5 Basket Size per Order (Prior)
| Metric | Value |
|---|---|
| Min products/order | 1 |
| Max products/order | 145 |
| Avg products/order | ~10.09 |

> [!NOTE]
> Single-item orders exist (min=1). These are legitimate but "thin" orders that may limit reorder pattern analysis. The distribution is likely **right-skewed** with a long tail of large baskets.

### 6.6 Product Coverage
- Unique products ordered in prior: **49,677** out of 49,688 in catalog
- Prior `product_id` values not in `products.csv`: **0** ✅ — Perfect referential integrity

---

## 7. `order_products__train.csv` — Detailed Analysis

### 7.1 Null / Missing Values
All columns: **0 nulls** ✅

### 7.2 Scale
- **1,384,617 rows** across **131,209 train orders**
- Average of ~10.55 products per order (slightly larger baskets than prior)

### 7.3 `reordered` Distribution
| Value | Count | % |
|---|---|---|
| 0 (new product) | 555,793 | 40.1% |
| 1 (reordered) | 828,824 | 59.9% |

> [!NOTE]
> The reorder rate in train (~60%) is consistent with the prior set (~59%). This confirms the train set is a representative sample of the same user behavior — no distribution shift detected on this dimension.

### 7.4 `add_to_cart_order` Statistics
| Metric | Value |
|---|---|
| Min | 1 |
| Max | 80 |
| Mean | ~8.76 |

> [!NOTE]
> The train set's max cart position (80) is lower than prior's (145). This is expected since the train set is a single "most recent" order per user, which will tend to be more typical-sized than the full prior history (which includes early, exploratory large orders).

### 7.5 `add_to_cart_order` Continuity
- Orders with gaps in sequence: **0** ✅ — sequences are always consecutive, no skipped positions

### 7.6 Product Coverage
- Unique products in train: **39,123** (out of 49,688) — only 78.8% of the catalog appears in train
- Train `product_id` values not in `products.csv`: **0** ✅ — No FK violations

> [!IMPORTANT]
> **10,565 products (21.2%) in the catalog do not appear in any train order.** These are products that exist in the prior set but were not part of any user's most recent order. When building recommendation or reorder-prediction models, this creates a **cold-start problem at the train-set level** — these products will have no training label signal.

---

## 8. `aisles.csv` & `departments.csv` — Detailed Analysis

### 8.1 Aisles (134 rows)
- No nulls, no duplicates
- IDs range 1–134, fully contiguous
- Notable categories with data quality concern:
  - `aisle_id = 100` → **"missing"** (placeholder)
  - `aisle_id = 6` → **"other"** (catch-all)

### 8.2 Departments (21 rows)
- No nulls, no duplicates
- IDs range 1–21, fully contiguous
- Notable categories with data quality concern:
  - `department_id = 21` → **"missing"** (placeholder)
  - `department_id = 2` → **"other"** (catch-all)

---

## 9. Cross-Table Issues & Referential Integrity Summary

| Check | Result |
|---|---|
| `products.aisle_id` → `aisles.aisle_id` | ✅ 0 violations |
| `products.department_id` → `departments.department_id` | ✅ 0 violations |
| `order_products__prior.product_id` → `products.product_id` | ✅ 0 violations |
| `order_products__train.product_id` → `products.product_id` | ✅ 0 violations |
| `order_products__train.order_id` → `orders.order_id` | ✅ 0 violations |
| Duplicate `order_id` in `orders.csv` | ✅ 0 duplicates |
| `order_number` sequential continuity per user | ✅ All clean |
| `add_to_cart_order` sequential continuity in train | ✅ 0 gaps |
| Products in catalog never ordered | ⚠️ 3 cold products (IDs: 3630, 46625, 7045) |
| Products in train not in catalog | ✅ 0 violations |

---

## 10. Summary of All Issues Found

### 🔴 Critical Issues (must address before modeling)

| # | Issue | Affected Table | Affected Column | Scope | Recommended Fix in PySpark |
|---|---|---|---|---|---|
| 1 | **`days_since_prior_order` structurally null for all first orders** | `orders.csv` | `days_since_prior_order` | 206,209 rows (6.03%) | Fill nulls with `0` or `-1` as a sentinel; OR add a boolean flag column `is_first_order` |
| 2 | **`days_since_prior_order` hard-capped at 30.0** | `orders.csv` | `days_since_prior_order` | 369,323 rows (11.5% of non-null) | Add a binary flag `days_capped = (days_since_prior_order == 30)` to preserve this information |
| 3 | **Product names with embedded commas** | `products.csv` | `product_name` | ~hundreds of products | Use PySpark's native CSV reader with proper quoting — never split on raw commas |
| 4 | **"missing" and "other" placeholder categories** | `products.csv`, `aisles.csv`, `departments.csv` | `aisle_id`, `department_id` | 1,806 products (3.63%) | Treat as their own category class OR exclude from category-based features |

### 🟡 Important Issues (impact analysis and features)

| # | Issue | Affected Table | Scope | Recommended Fix |
|---|---|---|---|---|
| 5 | **`order_dow` encoding undocumented** | `orders.csv` | 3.4M rows | Document the mapping (0=Sunday likely); treat as nominal categorical |
| 6 | **`days_since_prior_order = 0`** (same-day orders) | `orders.csv` | 67,755 rows | Investigate; flag with `same_day_reorder` boolean if confirmed legitimate |
| 7 | **Extreme basket sizes (max 145 items in prior)** | `order_products__prior.csv` | Long tail of large orders | Investigate frequency; consider capping or flagging orders with >50 products as outliers |
| 8 | **10,565 products absent from train labels** | `order_products__train.csv` | 21.2% of catalog | Impute from prior set frequencies; apply cold-start strategy in model |
| 9 | **User bias toward engaged shoppers** (min 4 orders/user) | `orders.csv` | All 206,209 users | Document this filtering; avoid generalizing model predictions to new/occasional users |

### 🟢 Informational (no action required, but worth noting)

| # | Observation |
|---|---|
| 10 | 3 products in catalog were never ordered (IDs: 3630, 46625, 7045) — harmless noise in the product table |
| 11 | The `test` eval_set has no basket contents — product data for test orders is the prediction target |
| 12 | The dataset is split at the **user level**: each user contributes exactly 1 train OR 1 test order (not both) |
| 13 | `order_number` is perfectly contiguous per user — can be safely used for session ordering |
| 14 | `reorder` rate is consistent between prior (~59%) and train (~60%) — no distribution shift |

---

## 11. PySpark-Specific Considerations

> [!TIP]
> **Reading CSVs**: Always use `spark.read.csv(..., header=True, inferSchema=True, quote='"')` to handle quoted product names with commas. Never use `multiLine=True` unless newlines in strings are expected (they are not in this dataset).

> [!TIP]
> **Schema enforcement**: `inferSchema=True` may misread some columns (e.g., `days_since_prior_order` as Double, `reordered` as Integer). Defining an explicit **StructType schema** is recommended for production-grade pipelines to prevent silent type mismatches.

> [!TIP]
> **Partitioning strategy**: `order_products__prior.csv` at 578 MB (~32M rows) is the largest file. Partition by `user_id` (after joining with `orders.csv`) when doing user-level aggregations. Avoid repartitioning unnecessarily on `order_id` for the prior set as it creates too many small groups.

> [!TIP]
> **Join order**: Broadcast join `aisles`, `departments`, and `products` (all small tables) against the large fact tables. Avoid large-to-large joins where possible. `products.csv` at 49K rows fits comfortably in broadcast memory.

> [!IMPORTANT]
> **`order_products__prior.csv` vs `order_products__train.csv`**: These files have **identical schemas** but different semantics. They should be kept **separate** during modeling — the prior set provides historical features; the train set provides labels. Only union them if computing global product statistics.

---

## 12. Recommended Preprocessing Checklist for PySpark

- [ ] **Orders**: Fill `days_since_prior_order` nulls (first orders) with a sentinel (e.g., 0 or -1)
- [ ] **Orders**: Add `is_first_order` boolean flag
- [ ] **Orders**: Add `days_capped` boolean flag for `days_since_prior_order == 30`
- [ ] **Orders**: Resolve `order_dow` encoding — document 0=Sunday or validate against real timestamps if available
- [ ] **Orders**: Flag `same_day_reorder` where `days_since_prior_order == 0`
- [ ] **Products**: Parse CSV with proper quoting to handle comma-embedded product names
- [ ] **Products**: Assign a unified `"unknown"` label to products with `aisle_id=100` or `department_id=21`
- [ ] **Products**: Decide strategy for `aisle_id=6` / `department_id=2` ("other") products
- [ ] **Prior/Train**: Investigate and document orders with >50 products; consider outlier treatment
- [ ] **Train set**: Apply cold-start strategy for the 10,565 products with no train labels
- [ ] **Schema**: Define explicit PySpark StructType schemas for all 6 files
- [ ] **Validation**: After preprocessing, re-verify referential integrity and null counts

