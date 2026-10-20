import os
import pandas as pd

BASE_PATH = "data"
PROCESSED_DIR = os.path.join(BASE_PATH, "processed")
WAREHOUSE_DIR = os.path.join(BASE_PATH, "warehouse")


def bronze_layer():
    """Just load the raw generated CSVs, tag with ingestion time."""
    orders = pd.read_csv(os.path.join(PROCESSED_DIR, "orders.csv"))
    restaurants = pd.read_csv(os.path.join(PROCESSED_DIR, "restaurants.csv"))
    reviews = pd.read_csv(os.path.join(PROCESSED_DIR, "reviews.csv"))

    orders["_ingested_at"] = pd.Timestamp.now()
    restaurants["_ingested_at"] = pd.Timestamp.now()
    reviews["_ingested_at"] = pd.Timestamp.now()

    print(f"[BRONZE] orders={len(orders)} restaurants={len(restaurants)} reviews={len(reviews)}")
    return orders, restaurants, reviews


def silver_layer(orders, restaurants, reviews):
    """Clean, dedupe, flag outliers, normalize numeric columns."""
    orders = orders.drop_duplicates(subset=["order_id"]).copy()
    orders["order_ts"] = pd.to_datetime(orders["order_ts"])
    orders["order_date"] = orders["order_ts"].dt.date

    # outlier bounds via IQR
    def bounds(col):
        q1, q3 = orders[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        return q1 - 1.5 * iqr, q3 + 1.5 * iqr

    dist_lo, dist_hi = bounds("distance_km")
    dt_lo, dt_hi = bounds("delivery_time_min")

    orders["is_distance_outlier"] = (orders["distance_km"] < dist_lo) | (orders["distance_km"] > dist_hi)
    orders["is_delivery_time_outlier"] = (orders["delivery_time_min"] < dt_lo) | (orders["delivery_time_min"] > dt_hi)
    orders["weather"] = orders["weather"].fillna("Unknown")
    orders["traffic_level"] = orders["traffic_level"].fillna("Unknown")

    for c in ["distance_km", "delivery_time_min", "prep_time_min", "courier_experience_yrs"]:
        mn, mx = orders[c].min(), orders[c].max()
        orders[f"{c}_norm"] = 0.0 if mx == mn else (orders[c] - mn) / (mx - mn)

    restaurants = restaurants.drop_duplicates(subset=["restaurant_id"]).copy()
    reviews = reviews.drop_duplicates(subset=["review_id"]).copy()

    print(f"[SILVER] orders={len(orders)} restaurants={len(restaurants)} reviews={len(reviews)}")
    return orders, restaurants, reviews


def gold_layer(orders, restaurants, reviews):
    """Join into a fact table + daily summary."""
    fact_orders = (
        orders
        .merge(restaurants[["restaurant_id", "city", "cuisine"]], on="restaurant_id", how="left")
        .merge(reviews[["order_id", "liked"]], on="order_id", how="left")
    )
    fact_orders["date_key"] = pd.to_datetime(fact_orders["order_date"]).dt.strftime("%Y%m%d").astype(int)

    fact_orders = fact_orders[[
        "order_id", "restaurant_id", "date_key", "distance_km", "delivery_time_min",
        "prep_time_min", "courier_experience_yrs", "weather", "traffic_level", "vehicle_type",
        "time_of_day", "city", "cuisine", "liked", "is_distance_outlier", "is_delivery_time_outlier",
    ]]

    daily_summary = (
        fact_orders.groupby("date_key")
        .agg(
            total_orders=("order_id", "count"),
            avg_delivery_time_min=("delivery_time_min", "mean"),
            avg_distance_km=("distance_km", "mean"),
            pct_liked=("liked", "mean"),
        )
        .reset_index()
    )
    daily_summary["avg_delivery_time_min"] = daily_summary["avg_delivery_time_min"].round(1)
    daily_summary["avg_distance_km"] = daily_summary["avg_distance_km"].round(1)
    daily_summary["pct_liked"] = (daily_summary["pct_liked"] * 100).round(1)

    print(f"[GOLD] fact_orders={len(fact_orders)} daily_summary={len(daily_summary)}")
    return fact_orders, daily_summary


def run():
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)

    orders, restaurants, reviews = bronze_layer()
    orders, restaurants, reviews = silver_layer(orders, restaurants, reviews)
    fact_orders, daily_summary = gold_layer(orders, restaurants, reviews)

    fact_orders.to_csv(os.path.join(WAREHOUSE_DIR, "gold_fact_orders.csv"), index=False)
    daily_summary.to_csv(os.path.join(WAREHOUSE_DIR, "gold_daily_summary.csv"), index=False)


if __name__ == "__main__":
    run()
    