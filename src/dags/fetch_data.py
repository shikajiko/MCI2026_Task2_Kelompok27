from utils.orders_source import (
    fetch_orders_json,
    extract_orders,
    flatten_orders_to_rows,
    save_raw_json,
    save_raw_parquet,
)

def fetch_order_data():
    payload = fetch_orders_json()
    orders = extract_orders(payload)
    rows = flatten_orders_to_rows(orders)

    raw_path = save_raw_json(payload)
    parquet_path = save_raw_parquet(rows)

    print(f"Fetched orders: {len(orders)}")
    print(f"Flattened rows: {len(rows)}")
    print(f"Saved raw JSON: {raw_path}")
    print(f"Saved raw Parquet: {parquet_path}")


if __name__ == "__main__":
    fetch_order_data()
