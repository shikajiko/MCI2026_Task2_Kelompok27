from clickhouse_driver import Client
from pyspark.sql import SparkSession
import os


DATA_LAKE_DIR = os.environ.get(
    "ORDER_DATA_LAKE_DIR",
    "/opt/airflow/data_lake/orders"
)
CLEAN_PARQUET_DIR = os.path.join(DATA_LAKE_DIR, "clean_parquet")


def load_orders_to_clickhouse():
    spark = SparkSession.builder \
        .appName("Orders_Load_ClickHouse") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca data bersih dari clean_parquet...")
    df_clean = spark.read.parquet(f"file://{CLEAN_PARQUET_DIR}")

    final_results = df_clean.toPandas()
    spark.stop()

    print(f"Total baris yang akan dimuat: {len(final_results)}")

    print("Memuat ke ClickHouse Warehouse...")
    client = Client(
        host="clickhouse-server",
        user="admin",
        password="rahasia"
    )

    client.execute("CREATE DATABASE IF NOT EXISTS mci_task2")
    client.execute("DROP TABLE IF EXISTS mci_task2.orders_fact")

    client.execute('''
        CREATE TABLE mci_task2.orders_fact (
            order_id            Int32,
            user_id             Int32,
            order_number        Int32,
            order_dow           Int32,
            order_hour_of_day   Int32,
            days_since_prior_order Nullable(Float32),
            eval_set            String,
            product_id          Int32,
            product_name        String,
            aisle_id            Int32,
            aisle               String,
            department_id       Int32,
            department          Nullable(String),
            add_to_cart_order   Int32,
            reordered           Int32,
            is_days_since_prior_order_missing UInt8
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
    ''')

    load_columns = [
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
        "eval_set",
        "product_id",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "add_to_cart_order",
        "reordered",
        "is_days_since_prior_order_missing",
    ]

    final_results = final_results.astype(object).where(
        final_results.notnull(),
        None,
    )

    data_tuples = [
        tuple(row)
        for row in final_results[load_columns].itertuples(index=False, name=None)
    ]

    if data_tuples:
        client.execute(
            "INSERT INTO mci_task2.orders_fact VALUES",
            data_tuples,
        )

    print("✅ Load ke ClickHouse selesai!")


if __name__ == "__main__":
    load_orders_to_clickhouse()
