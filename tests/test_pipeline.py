import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from pipeline.generate_data import generate_restaurants, generate_orders, generate_reviews


def test_generate_restaurants_count():
    restaurants = generate_restaurants()
    assert len(restaurants) > 0
    assert "restaurant_id" in restaurants.columns


def test_generate_orders_linked_to_restaurants():
    restaurants = generate_restaurants()
    orders = generate_orders(restaurants)
    assert orders["restaurant_id"].isin(restaurants["restaurant_id"]).all()


def test_generate_reviews_linked_to_orders():
    restaurants = generate_restaurants()
    orders = generate_orders(restaurants)
    reviews = generate_reviews(orders)
    assert reviews["order_id"].isin(orders["order_id"]).all()
    assert len(reviews) > 0