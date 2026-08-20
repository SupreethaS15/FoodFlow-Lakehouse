# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Upload your 3 Kaggle CSVs (any filenames) to /dbfs/FileStore/food_delivery/real_data/ first:
#   - Zomato Restaurants Data
#   - Food Delivery Time Prediction
#   - Restaurant Reviews
# Files are auto-detected by column names, not filenames. Then run all cells.

# COMMAND ----------

import glob
import os
import pandas as pd

for f in glob.glob(os.path.join(REAL_DIR, "*.csv")):
    try:
        df = pd.read_csv(f, nrows=5, encoding="latin1")
        print("\nFILE:", os.path.basename(f))
        print("COLUMNS:", df.columns.tolist())
    except Exception as e:
        print("ERROR:", f, e)

# COMMAND ----------

import os, json, glob, random
from datetime import datetime, timedelta, timezone
import pandas as pd

random.seed(42)
BASE_PATH = "/Volumes/workspace/default/food_delivery_analytics/"          # -> /dbfs/FileStore/food_delivery on Databricks
REAL_DIR = os.path.join(BASE_PATH, "real_data")
NUM_DAYS = 21
TARGET_RESTAURANTS = 1000

def _load_by_signature(sig_cols):
    for f in glob.glob(os.path.join(REAL_DIR, "*.csv")):

        try:
            df = pd.read_csv(f, nrows=1, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(f, nrows=1, encoding="latin1")

        # Remove extra spaces from column names
        df.columns = df.columns.str.strip()

        if set(sig_cols).issubset(set(df.columns)):

            try:
                full_df = pd.read_csv(f, encoding="utf-8")
            except UnicodeDecodeError:
                full_df = pd.read_csv(f, encoding="latin1")

            # Remove extra spaces from actual column names
            full_df.columns = full_df.columns.str.strip()

            return full_df

    raise FileNotFoundError(
        f"No CSV in {REAL_DIR} has columns {sig_cols}"
    )
# ---- 1. Restaurants (Zomato) -> sample to 1000, keep essential columns ----
rest_raw = _load_by_signature(["Restaurant ID", "Cuisines", "Aggregate rating"])
rest_raw = rest_raw.sample(n=min(TARGET_RESTAURANTS, len(rest_raw)), random_state=42).reset_index(drop=True)
restaurants = pd.DataFrame({
    "restaurant_id": range(1, len(rest_raw) + 1),
    "name": rest_raw["Restaurant Name"],
    "city": rest_raw["City"],
    "cuisine": rest_raw["Cuisines"].astype(str).str.split(",").str[0].str.strip(),
    "cost_for_two": rest_raw["Average Cost for two"],
    "has_online_delivery": rest_raw["Has Online delivery"],
    "price_range": rest_raw["Price range"],
    "aggregate_rating": rest_raw["Aggregate rating"],
    "votes": rest_raw["Votes"],
})
print(f"[Restaurants] {len(restaurants)} rows (real: Zomato)")

# ---- 2. Orders/Deliveries -> distribute across NUM_DAYS, link to restaurants ----
delivery_raw = _load_by_signature(["Order_ID", "Distance_km", "Delivery_Time_min"])
TIME_MAP = {"Morning": (6, 11), "Afternoon": (12, 16), "Evening": (17, 20), "Night": (21, 23)}
today = datetime.now(timezone.utc).date()

def _rand_ts(day_idx, time_of_day):
    d = today - timedelta(days=NUM_DAYS - day_idx)
    lo, hi = TIME_MAP.get(time_of_day, (8, 22))
    return datetime.combine(d, datetime.min.time()) + timedelta(hours=random.randint(lo, hi), minutes=random.randint(0, 59))

orders = delivery_raw.rename(columns={
    "Order_ID": "order_id", "Distance_km": "distance_km", "Weather": "weather",
    "Traffic_Level": "traffic_level", "Time_of_Day": "time_of_day", "Vehicle_Type": "vehicle_type",
    "Preparation_Time_min": "prep_time_min", "Courier_Experience_yrs": "courier_experience_yrs",
    "Delivery_Time_min": "delivery_time_min",
}).copy()
orders["restaurant_id"] = [random.choice(restaurants["restaurant_id"]) for _ in range(len(orders))]
orders["order_ts"] = [_rand_ts(i % NUM_DAYS, t) for i, t in enumerate(orders["time_of_day"])]
orders["order_date"] = orders["order_ts"].dt.date
print(f"[Orders] {len(orders)} rows (real: Food Delivery Time Prediction) linked to {orders['restaurant_id'].nunique()} restaurants")

# ---- 3. Reviews -> shuffled 1:1 link to orders (no shared key in source data) ----
reviews_raw = _load_by_signature(["Review", "Liked"])
order_ids_shuffled = orders["order_id"].sample(frac=1, random_state=1).reset_index(drop=True)
reviews = pd.DataFrame({
    "review_id": range(1, len(reviews_raw) + 1),
    "order_id": order_ids_shuffled[:len(reviews_raw)].values,
    "review_text": reviews_raw["Review"],
    "liked": reviews_raw["Liked"],
})
print(f"[Reviews] {len(reviews)} rows (real: Restaurant Reviews) linked 1:1 to orders via random shuffle")

# ---- write outputs ----
nosql_dir = os.path.join(BASE_PATH, "nosql")
landing_dir = os.path.join(BASE_PATH, "landing", "orders")
reviews_dir = os.path.join(BASE_PATH, "landing", "reviews")
for d in (nosql_dir, landing_dir, reviews_dir):
    os.makedirs(d, exist_ok=True)

restaurants.to_json(os.path.join(nosql_dir, "restaurants.json"), orient="records", indent=2)

for date, day_df in orders.groupby("order_date"):
    day_df.drop(columns=["order_date"]).to_csv(os.path.join(landing_dir, f"orders_{date}.csv"), index=False)
reviews.to_csv(os.path.join(reviews_dir, "reviews.csv"), index=False)

manifest = {
    "restaurants": {"source": "real", "dataset": "Zomato Restaurants Data", "rows": len(restaurants)},
    "orders_deliveries": {"source": "real", "dataset": "Food Delivery Time Prediction", "rows": len(orders),
                           "adaptation": "order_date synthesized (distributed across a rolling 21-day window based on Time_of_Day); restaurant_id assigned randomly (source has no restaurant key)"},
    "reviews": {"source": "real", "dataset": "Restaurant Reviews (Kaggle)", "rows": len(reviews),
                "adaptation": "linked to orders via random 1:1 shuffle (source has no order key)"},
}
with open(os.path.join(BASE_PATH, "data_source_manifest.json"), "w") as f:
    json.dump(manifest, f, indent=2)
print("\nManifest:", json.dumps(manifest, indent=2))

# COMMAND ----------

# Databricks notebook source
# Bronze -> Silver -> Gold pipeline. Run 01_data_generator.py first.
# Ex-1: EDA (nulls, outliers, normalization) happens in the Silver step.
# Ex-2: extraction (Bronze) + transformation (Silver) + load (Gold), full load pattern.
# Ex-3: Gold is the Star Schema (fact_orders + dim_restaurant + dim_date).

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/default/food_delivery_analytics/"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.food_delivery")
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA food_delivery")

# COMMAND ----------

# MAGIC %md ## BRONZE — raw ingestion, as-is, with lineage metadata

# COMMAND ----------

# BRONZE — raw ingestion, as-is, with lineage metadata

bronze_orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE}/landing/orders/*.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

bronze_reviews = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE}/landing/reviews/reviews.csv")
    .withColumn("_ingested_at", F.current_timestamp())
)

bronze_restaurants = (
    spark.read
    .option("multiLine", True)
    .json(f"{BASE}/nosql/restaurants.json")
    .withColumn("_ingested_at", F.current_timestamp())
)

bronze_orders.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
bronze_reviews.write.format("delta").mode("overwrite").saveAsTable("bronze_reviews")
bronze_restaurants.write.format("delta").mode("overwrite").saveAsTable("bronze_restaurants")

print(
    f"[BRONZE] orders={bronze_orders.count()} "
    f"reviews={bronze_reviews.count()} "
    f"restaurants={bronze_restaurants.count()}"
)

# COMMAND ----------

# MAGIC %md ## SILVER — EDA + cleaning + normalization (Ex-1)

# COMMAND ----------

orders_raw = spark.table("bronze_orders")
total = orders_raw.count()

# --- null-rate EDA report ---
null_report = orders_raw.select([
    (F.sum(F.col(c).isNull().cast("int")) / total).alias(c)
    for c in orders_raw.columns if not c.startswith("_")
])
print("[EDA] null rate per column:")
null_report.show(vertical=True, truncate=False)

# --- IQR outlier bounds ---
def iqr_bounds(df, col):
    q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr

dist_lo, dist_hi = iqr_bounds(orders_raw, "distance_km")
dt_lo, dt_hi = iqr_bounds(orders_raw, "delivery_time_min")

# COMMAND ----------

silver_orders = (orders_raw
    .dropDuplicates(["order_id"])
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("is_distance_outlier", (F.col("distance_km") < dist_lo) | (F.col("distance_km") > dist_hi))
    .withColumn("is_delivery_time_outlier", (F.col("delivery_time_min") < dt_lo) | (F.col("delivery_time_min") > dt_hi))
    .fillna({"weather": "Unknown", "traffic_level": "Unknown"}))

# min-max normalization
for c in ["distance_km", "delivery_time_min", "prep_time_min", "courier_experience_yrs"]:
    mn, mx = silver_orders.agg(F.min(c), F.max(c)).first()
    silver_orders = silver_orders.withColumn(f"{c}_norm", (F.col(c) - mn) / (mx - mn))

silver_restaurants = (spark.table("bronze_restaurants")
    .dropDuplicates(["restaurant_id"])
    .withColumn("cost_for_two", F.col("cost_for_two").cast("double"))
    .withColumn("aggregate_rating", F.col("aggregate_rating").cast("double")))

silver_reviews = (
    spark.table("bronze_reviews")
    .dropDuplicates(["review_id"])

    .withColumn(
        "review_text_clean",
        F.lower(
            F.trim(
                F.col("review_text")
            )
        )
    )

    .withColumn(
        "liked",
        F.when(
            F.lower(F.trim(F.col("liked").cast("string"))).isin(
                "yes", "true", "1", "liked"
            ),
            1
        )
        .when(
            F.lower(F.trim(F.col("liked").cast("string"))).isin(
                "no", "false", "0", "not liked"
            ),
            0
        )
        .otherwise(None)
        .cast("int")
    )
)

silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
silver_restaurants.write.format("delta").mode("overwrite").saveAsTable("silver_restaurants")
silver_reviews.write.format("delta").mode("overwrite").saveAsTable("silver_reviews")
print(f"[SILVER] orders={silver_orders.count()} outliers_flagged={silver_orders.filter('is_distance_outlier or is_delivery_time_outlier').count()}")

# COMMAND ----------

# MAGIC %md ## GOLD — Star Schema (Ex-3): fact_orders + dim_restaurant + dim_date

# COMMAND ----------

so = spark.table("silver_orders")
sr = spark.table("silver_restaurants")
srev = spark.table("silver_reviews")

dim_restaurant = sr.select("restaurant_id", "name", "city", "cuisine", "cost_for_two",
                            "aggregate_rating", "votes", "has_online_delivery", "price_range")

dim_date = (so.select("order_date").distinct()
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("day_name", F.date_format("order_date", "EEEE"))
    .withColumn("month", F.month("order_date"))
    .withColumn("is_weekend", F.dayofweek("order_date").isin([1, 7])))

fact_orders = (so
    .join(dim_restaurant.select("restaurant_id", "city", "cuisine"), "restaurant_id", "left")
    .join(srev.select("order_id", "liked"), "order_id", "left")
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .select("order_id", "restaurant_id", "date_key", "distance_km", "delivery_time_min",
            "prep_time_min", "courier_experience_yrs", "weather", "traffic_level", "vehicle_type",
            "time_of_day", "city", "cuisine", "liked", "is_distance_outlier", "is_delivery_time_outlier"))

daily_summary = (fact_orders.groupBy("date_key").agg(
    F.count("*").alias("total_orders"),
    F.round(F.avg("delivery_time_min"), 1).alias("avg_delivery_time_min"),
    F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
    F.round(F.avg("liked") * 100, 1).alias("pct_liked")))

for name, df in [("dim_restaurant", dim_restaurant), ("dim_date", dim_date),
                  ("fact_orders", fact_orders), ("daily_summary", daily_summary)]:
    df.write.format("delta").mode("overwrite").saveAsTable(f"gold_{name}")
    print(f"[GOLD] {name}: {df.count()} rows")

display(spark.table("gold_daily_summary").orderBy("date_key"))

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists("processed_files"):
    spark.createDataFrame([], "file_path STRING, processed_at TIMESTAMP") \
        .write.format("delta").saveAsTable("processed_files")

processed = set(r.file_path for r in spark.table("processed_files").select("file_path").collect())
all_files = [f.path for f in dbutils.fs.ls(f"{BASE}/landing/orders/")]
new_files = [f for f in all_files if f not in processed]

if new_files:
    new_orders = (spark.read.option("header", True).option("inferSchema", True).csv(new_files)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path")))

    if spark.catalog.tableExists("bronze_orders"):
        new_orders.write.format("delta").mode("append").saveAsTable("bronze_orders")
    else:
        new_orders.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")

    spark.createDataFrame([(f,) for f in new_files], "file_path STRING") \
        .withColumn("processed_at", F.current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("processed_files")

    print(f"[BRONZE] ingested {len(new_files)} new file(s), {new_orders.count()} new rows")
else:
    print("[BRONZE] no new files, nothing to do")

# COMMAND ----------

new_bronze = spark.table("bronze_orders") if new_files else spark.table("bronze_orders").limit(0)

silver_orders_new = (new_bronze
    .dropDuplicates(["order_id"])
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("is_distance_outlier", (F.col("distance_km") < dist_lo) | (F.col("distance_km") > dist_hi))
    .withColumn("is_delivery_time_outlier", (F.col("delivery_time_min") < dt_lo) | (F.col("delivery_time_min") > dt_hi))
    .fillna({"weather": "Unknown", "traffic_level": "Unknown"}))

for c in ["distance_km", "delivery_time_min", "prep_time_min", "courier_experience_yrs"]:
    mn, mx = silver_orders_new.agg(F.min(c), F.max(c)).first()
    silver_orders_new = silver_orders_new.withColumn(f"{c}_norm", (F.col(c) - mn) / (mx - mn) if mx != mn else F.lit(0.0))

if not spark.catalog.tableExists("silver_orders"):
    silver_orders_new.write.format("delta").saveAsTable("silver_orders")
else:
    target = DeltaTable.forName(spark, "silver_orders")
    (target.alias("t")
        .merge(silver_orders_new.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

print("[SILVER] upsert complete —", spark.table("silver_orders").count(), "total rows")

# COMMAND ----------

so = spark.table("silver_orders")
sr = spark.table("silver_restaurants")
srev = spark.table("silver_reviews")

fact_orders = (so
    .join(sr.select("restaurant_id", "city", "cuisine"), "restaurant_id", "left")
    .join(srev.select("order_id", "liked"), "order_id", "left")
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .select("order_id", "restaurant_id", "date_key", "distance_km", "delivery_time_min",
            "prep_time_min", "courier_experience_yrs", "weather", "traffic_level", "vehicle_type",
            "time_of_day", "city", "cuisine", "liked", "is_distance_outlier", "is_delivery_time_outlier"))

daily_summary = (fact_orders.groupBy("date_key").agg(
    F.count("*").alias("total_orders"),
    F.round(F.avg("delivery_time_min"), 1).alias("avg_delivery_time_min"),
    F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
    F.round(F.avg("liked") * 100, 1).alias("pct_liked")))

fact_orders.write.format("delta").mode("overwrite").saveAsTable("gold_fact_orders")
daily_summary.write.format("delta").mode("overwrite").saveAsTable("gold_daily_summary")
print(f"[GOLD] fact_orders={fact_orders.count()} daily_summary={daily_summary.count()}")