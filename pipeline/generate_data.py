import glob
import os
import random
from datetime import datetime, timedelta, timezone
import pandas as pd

random.seed(42)

BASE_PATH = "data"
REAL_DIR = os.path.join(BASE_PATH, "real_data")
NUM_DAYS = 21
TARGET_RESTAURANTS = 1000


def _load_by_signature(sig_cols):
    for f in glob.glob(os.path.join(REAL_DIR, "*.csv")):
        try:
            df = pd.read_csv(f, nrows=1, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(f, nrows=1, encoding="latin1")
        df.columns = df.columns.str.strip()
        if set(sig_cols).issubset(set(df.columns)):
            try:
                full_df = pd.read_csv(f, encoding="utf-8")
            except UnicodeDecodeError:
                full_df = pd.read_csv(f, encoding="latin1")
            full_df.columns = full_df.columns.str.strip()
            return full_df
    raise FileNotFoundError(f"No CSV in {REAL_DIR} has columns {sig_cols}")


def generate_restaurants():
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
    return restaurants


def generate_orders(restaurants):
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
    orders["order_date"] = pd.to_datetime(orders["order_ts"]).dt.date
    return orders


def generate_reviews(orders):
    reviews_raw = _load_by_signature(["Review", "Liked"])
    order_ids_shuffled = orders["order_id"].sample(frac=1, random_state=1).reset_index(drop=True)
    reviews = pd.DataFrame({
        "review_id": range(1, len(reviews_raw) + 1),
        "order_id": order_ids_shuffled[:len(reviews_raw)].values,
        "review_text": reviews_raw["Review"],
        "liked": reviews_raw["Liked"],
    })
    return reviews


def run():
    restaurants = generate_restaurants()
    orders = generate_orders(restaurants)
    reviews = generate_reviews(orders)

    out_dir = os.path.join(BASE_PATH, "processed")
    os.makedirs(out_dir, exist_ok=True)
    restaurants.to_csv(os.path.join(out_dir, "restaurants.csv"), index=False)
    orders.to_csv(os.path.join(out_dir, "orders.csv"), index=False)
    reviews.to_csv(os.path.join(out_dir, "reviews.csv"), index=False)

    print(f"[Restaurants] {len(restaurants)} rows")
    print(f"[Orders] {len(orders)} rows linked to {orders['restaurant_id'].nunique()} restaurants")
    print(f"[Reviews] {len(reviews)} rows")

if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from pipeline.transform import run as run_transform

    run()
    run_transform()