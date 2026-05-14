import requests
import pandas as pd
import os
from datetime import datetime

DATA_LAKE_DIR = os.environ.get(
    "ORDER_DATA_LAKE_DIR"
    "/opt/airflow/data_lake/orders"
)

def fetch_order_data():
    url = "http://96.9.212.102:8000/orders"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        orders = data["orders"]

        parsed_data = []

        for order in orders:
            for product in order.get("products", []):
                parsed_data.append({
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

        df = pd.DataFrame(parsed_data)

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(DATA_LAKE_DIR, f"orders_{current_time}.parquet")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False)

        print(f"Successfully fetched {len(df)} data to {output_path}")

    except Exception as e:
        print(f"Failed to fetch data: {e}")
        raise

if __name__ == "__main__":
    fetch_order_data()
