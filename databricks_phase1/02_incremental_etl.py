# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup and Configuration
# Incremental ETL Pipeline
# Run this on a schedule to process new data automatically

from pyspark.sql import functions as F
from delta.tables import DeltaTable

BASE = "/Volumes/workspace/default/food_delivery_analytics/"

# Use existing schema
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA food_delivery")

print("[SETUP] Connected to workspace.food_delivery")

# COMMAND ----------

# DBTITLE 1,Bronze: Incremental File Ingestion
# Track which files have been processed
if not spark.catalog.tableExists("processed_files"):
    spark.createDataFrame([], "file_path STRING, processed_at TIMESTAMP") \
        .write.format("delta").saveAsTable("processed_files")

# Find new files
processed = set(r.file_path for r in spark.table("processed_files").select("file_path").collect())
all_files = [f.path for f in dbutils.fs.ls(f"{BASE}/landing/orders/")]
new_files = [f for f in all_files if f not in processed]

if new_files:
    # Ingest new files
    new_orders = (spark.read.option("header", True).option("inferSchema", True).csv(new_files)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path")))

    if spark.catalog.tableExists("bronze_orders"):
        new_orders.write.format("delta").mode("append").saveAsTable("bronze_orders")
    else:
        new_orders.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")

    # Mark files as processed
    spark.createDataFrame([(f,) for f in new_files], "file_path STRING") \
        .withColumn("processed_at", F.current_timestamp()) \
        .write.format("delta").mode("append").saveAsTable("processed_files")

    print(f"[BRONZE] Ingested {len(new_files)} new file(s), {new_orders.count()} new rows")
    new_rows_count = new_orders.count()
    newly_processed_files = new_files  # Track for Silver layer
else:
    print("[BRONZE] No new files found")
    new_rows_count = 0
    newly_processed_files = []

# COMMAND ----------

# DBTITLE 1,Silver: Transform and Upsert
if new_rows_count > 0:
    # Get IQR bounds for outlier detection (use existing data)
    orders_raw = spark.table("bronze_orders")
    
    def iqr_bounds(df, col):
        q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        return q1 - 1.5 * iqr, q3 + 1.5 * iqr
    
    dist_lo, dist_hi = iqr_bounds(orders_raw, "distance_km")
    dt_lo, dt_hi = iqr_bounds(orders_raw, "delivery_time_min")
    
    # Transform new data - filter by the files that were just processed
    new_bronze = spark.table("bronze_orders").filter(F.col("_source_file").isin(newly_processed_files))
    
    silver_orders_new = (new_bronze
        .dropDuplicates(["order_id"])
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("is_distance_outlier", (F.col("distance_km") < dist_lo) | (F.col("distance_km") > dist_hi))
        .withColumn("is_delivery_time_outlier", (F.col("delivery_time_min") < dt_lo) | (F.col("delivery_time_min") > dt_hi))
        .fillna({"weather": "Unknown", "traffic_level": "Unknown"}))
    
    # Normalize numeric columns
    for c in ["distance_km", "delivery_time_min", "prep_time_min", "courier_experience_yrs"]:
        mn, mx = silver_orders_new.agg(F.min(c), F.max(c)).first()
        if mx != mn:
            silver_orders_new = silver_orders_new.withColumn(f"{c}_norm", (F.col(c) - mn) / (mx - mn))
        else:
            silver_orders_new = silver_orders_new.withColumn(f"{c}_norm", F.lit(0.0))
    
    # Upsert into silver
    if not spark.catalog.tableExists("silver_orders"):
        silver_orders_new.write.format("delta").saveAsTable("silver_orders")
    else:
        target = DeltaTable.forName(spark, "silver_orders")
        (target.alias("t")
            .merge(silver_orders_new.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    
    print(f"[SILVER] Upserted {silver_orders_new.count()} rows, total: {spark.table('silver_orders').count()}")
else:
    print("[SILVER] No new data to process")

# COMMAND ----------

# DBTITLE 1,Gold: Refresh Star Schema
if new_rows_count > 0:
    # Rebuild gold tables
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
    
    print(f"[GOLD] Updated fact_orders={fact_orders.count()}, daily_summary={daily_summary.count()}")
    print("\n✅ Pipeline completed successfully!")
else:
    print("[GOLD] No updates needed")
    print("\n⏸️  Pipeline completed - no new data processed")

# COMMAND ----------

