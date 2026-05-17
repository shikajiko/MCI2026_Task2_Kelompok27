import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


ORDER_API_URL = "http://96.9.212.102:8000/orders"

DATA_LAKE_DIR = os.environ.get(
    "ORDER_DATA_LAKE_DIR",
    "/opt/airflow/data_lake/orders"
)

def fetch_orders_json():
    response = requests.get(ORDER_API_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def extract_orders(payload):
    if "orders" not in payload:
        raise ValueError("orders tidak ditemukan")

    orders = payload["orders"]
    return orders


def flatten_orders_to_rows(orders):
    rows = []
    for order in orders:
        products = order.get("products", [])

        for product in products:
            rows.append({
                "order_id": order.get("order_id"),
                "user_id": order.get("user_id"),
                "order_number": order.get("order_number"),
                "order_dow": order.get("order_dow"),
                "order_hour_of_day": order.get("order_hour_of_day"),
                "days_since_prior_order": order.get("days_since_prior_order"),
                "eval_set": order.get("eval_set"),

                "product_id": product.get("product_id"),
                "product_name": product.get("product_name"),
                "aisle_id": product.get("aisle_id"),
                "aisle": product.get("aisle"),
                "department_id": product.get("department_id"),
                "department": product.get("department"),
                "add_to_cart_order": product.get("add_to_cart_order"),
                "reordered": product.get("reordered"),
            })

    return rows


def save_raw_json(payload):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_dir = Path(DATA_LAKE_DIR) / "raw_json"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"orders_raw_{timestamp}.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return str(output_path)


def save_raw_parquet(rows: list[dict]):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_dir = Path(DATA_LAKE_DIR) / "bronze_parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"orders_{timestamp}.parquet"

    df = pd.DataFrame(rows)
    df.to_parquet(output_path, index=False)

    return str(output_path)
