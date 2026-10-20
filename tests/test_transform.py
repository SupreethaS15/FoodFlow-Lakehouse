import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from pipeline.transform import silver_layer, gold_layer


def _sample_data():
    orders = pd.DataFrame({
        "order_id": list(range(1, 9)),
        "restaurant_id": [1, 1, 2, 1, 2, 1, 2, 1],
        "distance_km": [2.0, 3.0, 2.5, 3.5, 2.8, 3.2, 2.1, 100.0],  # 100 is the outlier
        "delivery_time_min": [20, 25, 22, 24, 21, 23, 22, 26],
        "prep_time_min": [10, 12, 11, 13, 10, 11, 12, 10],
        "courier_experience_yrs": [1, 2, 3, 1, 2, 3, 1, 2],
        "weather": ["Clear", None, "Rain", "Clear", "Clear", "Rain", "Clear", "Clear"],
        "traffic_level": ["Low", "High", None, "Low", "Medium", "High", "Low", "Medium"],
        "vehicle_type": ["Bike", "Car", "Bike", "Car", "Bike", "Car", "Bike", "Car"],
        "time_of_day": ["Morning", "Evening", "Night", "Morning", "Evening", "Night", "Morning", "Evening"],
        "order_ts": [
            "2024-01-01 08:00", "2024-01-02 18:00", "2024-01-03 22:00", "2024-01-04 08:00",
            "2024-01-05 18:00", "2024-01-06 22:00", "2024-01-07 08:00", "2024-01-08 18:00",
        ],
    })
    restaurants = pd.DataFrame({
        "restaurant_id": [1, 2],
        "city": ["CityA", "CityB"],
        "cuisine": ["Italian", "Chinese"],
    })
    reviews = pd.DataFrame({
        "review_id": list(range(1, 9)),
        "order_id": list(range(1, 9)),
        "liked": [1, 0, 1, 1, 0, 1, 1, 0],
    })
    return orders, restaurants, reviews


def test_silver_layer_fills_missing_and_flags_outliers():
    orders, restaurants, reviews = _sample_data()
    s_orders, s_rest, s_rev = silver_layer(orders, restaurants, reviews)
    assert s_orders["weather"].isnull().sum() == 0
    assert s_orders["traffic_level"].isnull().sum() == 0
    assert s_orders.loc[s_orders["order_id"] == 8, "is_distance_outlier"].iloc[0] == True


def test_gold_layer_joins_correctly():
    orders, restaurants, reviews = _sample_data()
    s_orders, s_rest, s_rev = silver_layer(orders, restaurants, reviews)
    fact_orders, daily_summary = gold_layer(s_orders, s_rest, s_rev)
    assert "city" in fact_orders.columns
    assert "liked" in fact_orders.columns
    assert len(daily_summary) > 0
    assert daily_summary["total_orders"].sum() == len(fact_orders)