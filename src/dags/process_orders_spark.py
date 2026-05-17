from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from clickhouse_driver import Client
import os
import glob

DATA_LAKE_DIR = os.environ.get(
    "ORDER_DATA_LAKE_DIR", 
    "/opt/airflow/data_lake/orders"
)

def run_orders_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Pipeline") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    print("Membaca seluruh aliran data dari Data Lake...")
    df_raw = spark.read.parquet(f"file://{DATA_LAKE_DIR}")

    # Transform (Berbeda dari contoh GitHub MCI) 
    print("Membersihkan data...")

    df_clean = df_raw \
        .dropna(subset=["order_id", "product_id"]) \
        .withColumn("order_id",             F.col("order_id").cast("int")) \
        .withColumn("user_id",              F.col("user_id").cast("int")) \
        .withColumn("order_number",         F.col("order_number").cast("int")) \
        .withColumn("order_dow",            F.col("order_dow").cast("int")) \
        .withColumn("order_hour_of_day",    F.col("order_hour_of_day").cast("int")) \
        .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("float")) \
        .withColumn("product_id",           F.col("product_id").cast("int")) \
        .withColumn("aisle_id",             F.col("aisle_id").cast("int")) \
        .withColumn("department_id",        F.col("department_id").cast("int")) \
        .withColumn("add_to_cart_order",    F.col("add_to_cart_order").cast("int")) \
        .withColumn("reordered",            F.col("reordered").cast("int"))

    final_results = df_clean.toPandas()
    spark.stop()

    print(f"total baris setelah transform: {len(final_results)}")
    # --Transform

    print("Memuat ke ClickHouse Warehouse...")

    client = Client(
        host="clickhouse-server",
        user="admin",
        password="rahasia"
    )

    client.execute("CREATE DATABASE IF NOT EXISTS mci_task2")

    client.execute('''
        CREATE TABLE IF NOT EXISTS mci_task2.orders_fact (
            order_id            Int32,
            user_id             Int32,
            order_number        Int32,
            order_dow           Int32,
            order_hour_of_day   Int32,
            days_since_prior_order Float32,
            eval_set            String,
            product_id          Int32,
            product_name        String,
            aisle_id            Int32,
            aisle               String,
            department_id       Int32,
            department          String,
            add_to_cart_order   Int32,
            reordered           Int32
        ) ENGINE = MergeTree()
        ORDER BY (order_id, product_id)
    ''')

    client.execute("TRUNCATE TABLE mci_task2.orders_fact")

    data_tuples = [tuple(x) for x in final_results.to_numpy()]
    if data_tuples:
        client.execute('INSERT INTO mci_task2.orders_fact VALUES', data_tuples)
       

    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob("/opt/airflow/data_lake/orders/*.parquet")

    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")
    
    print("✅ Pipeline Selesai!")

if __name__ == "__main__":
    run_orders_analytics()
