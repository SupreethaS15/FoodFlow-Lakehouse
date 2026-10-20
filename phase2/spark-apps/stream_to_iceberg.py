from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, TimestampType)

spark = (SparkSession.builder.appName("food-delivery-stream")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
  .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
  .config("spark.sql.catalog.iceberg.warehouse", "s3://lakehouse/")
  .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
  .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
  .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
  .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password")
  .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
  .getOrCreate())

# EDIT HERE if your orders.csv header differs

COLS = {
    "order_id": "string",
    "distance_km": "double",
    "weather": "string",
    "traffic_level": "string",
    "time_of_day": "string",
    "vehicle_type": "string",
    "prep_time_min": "double",
    "courier_experience_yrs": "double",
    "delivery_time_min": "double",
    "restaurant_id": "string",
    "order_ts": "string",
    "order_date": "string",
    "event_ts": "string"
}

PARTITION = "time_of_day"

spark_types = {"string": StringType(), "double": DoubleType()}
schema = StructType([StructField(c, spark_types[t]) for c, t in COLS.items()])
ddl_cols = ", ".join(f"{c} {t}" for c, t in COLS.items())

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.food_delivery")
spark.sql(f"""CREATE TABLE IF NOT EXISTS iceberg.food_delivery.orders_stream
  ({ddl_cols}, ingested_at timestamp) USING iceberg PARTITIONED BY ({PARTITION})""")
spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.food_delivery.dead_letter
  (raw_value string, failed_at timestamp) USING iceberg""")


def route(df, batch_id):
    parsed = (
        df.selectExpr("CAST(value AS STRING) AS raw_value")
          .withColumn("j", F.from_json("raw_value", schema))
    )

    good = (
        parsed.filter("j.order_id IS NOT NULL")
              .select("j.*")
              .dropDuplicates(["order_id"])
              .withColumn("ingested_at", F.current_timestamp())
    )

    bad = (
        parsed.filter("j.order_id IS NULL")
              .select("raw_value")
              .withColumn("failed_at", F.current_timestamp())
    )

    batch_spark = good.sparkSession
    view_name = f"batch_good_{batch_id}"

    try:
        # Register the view BEFORE executing MERGE.
        good.createOrReplaceGlobalTempView(view_name)

        print(
            f"Processing batch {batch_id}; "
            f"source=global_temp.{view_name}",
            flush=True
        )

        merge_sql = f"""
        MERGE INTO iceberg.food_delivery.orders_stream t
        USING global_temp.{view_name} s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET
            t.restaurant_id = s.restaurant_id,
            t.order_ts = s.order_ts,
            t.order_date = s.order_date,
            t.distance_km = s.distance_km,
            t.weather = s.weather,
            t.traffic_level = s.traffic_level,
            t.time_of_day = s.time_of_day,
            t.vehicle_type = s.vehicle_type,
            t.prep_time_min = s.prep_time_min,
            t.courier_experience_yrs = s.courier_experience_yrs,
            t.delivery_time_min = s.delivery_time_min,
            t.event_ts = s.event_ts,
            t.ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT (
            order_id, restaurant_id, order_ts, order_date,
            distance_km, weather, traffic_level, time_of_day,
            vehicle_type, prep_time_min, courier_experience_yrs,
            delivery_time_min, event_ts, ingested_at
        ) VALUES (
            s.order_id, s.restaurant_id, s.order_ts, s.order_date,
            s.distance_km, s.weather, s.traffic_level, s.time_of_day,
            s.vehicle_type, s.prep_time_min, s.courier_experience_yrs,
            s.delivery_time_min, s.event_ts, s.ingested_at
        )
        """

        batch_spark.sql(merge_sql)

        # Store malformed records in the dead-letter table.
        if not bad.isEmpty():
            (bad.writeTo("iceberg.food_delivery.dead_letter")
                .append())

        print(f"Batch {batch_id} completed successfully.", flush=True)

    finally:
        batch_spark.catalog.dropGlobalTempView(view_name)

raw = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "kafka:9092")
  .option("subscribe", "food-delivery-orders")
  .option("startingOffsets", "earliest").load())

(raw.writeStream.foreachBatch(route)
  .option("checkpointLocation", "/tmp/checkpoints/orders")
  .trigger(processingTime="10 seconds").start().awaitTermination())