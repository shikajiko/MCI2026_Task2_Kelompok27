from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
import os

DATA_LAKE_DIR = os.environ.get(
    "ORDER_DATA_LAKE_DIR", 
    "/opt/airflow/data_lake/orders"
)
RAW_PARQUET_DIR = os.path.join(DATA_LAKE_DIR, "raw_parquet")
CLEAN_PARQUET_DIR = os.path.join(DATA_LAKE_DIR, "clean_parquet")
REJECTED_PARQUET_DIR = os.path.join(DATA_LAKE_DIR, "rejected_parquet")

def run_orders_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Pipeline") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    print("Membaca raw parquet dari Data Lake...")
    df_raw = spark.read.parquet(f"file://{RAW_PARQUET_DIR}")

    total_raw = df_raw.count()
    print(f"Total baris raw: {total_raw}")

    # Transform dan validasi mengikuti hasil EDA:
    # - days_since_prior_order boleh null
    # - kolom kunci order dan product wajib ada
    # - nilai jam, hari, reordered, dan posisi keranjang harus masuk akal
    print("Membersihkan dan memvalidasi data...")

    df_typed = df_raw \
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

    required_cols = [
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "product_id",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "add_to_cart_order",
        "reordered",
    ]

    required_valid = None
    for col_name in required_cols:
        col_valid = F.col(col_name).isNotNull()
        required_valid = col_valid if required_valid is None else required_valid & col_valid

    range_valid = (
        (F.col("order_number") >= 1)
        & F.col("order_dow").between(0, 6)
        & F.col("order_hour_of_day").between(0, 23)
        & (F.col("add_to_cart_order") >= 1)
        & F.col("reordered").isin(0, 1)
        & (
            F.col("days_since_prior_order").isNull()
            | F.col("days_since_prior_order").between(0, 30)
        )
    )

    df_validated = df_typed.withColumn(
        "is_days_since_prior_order_missing",
        F.col("days_since_prior_order").isNull(),
    ).withColumn(
        "department",
        F.when(F.lower(F.trim(F.col("department"))) == "missing", None)
        .otherwise(F.col("department")),
    )

    df_clean = df_validated.filter(required_valid & range_valid)
    df_rejected = df_validated.filter(~(required_valid & range_valid))

    total_clean = df_clean.count()
    total_rejected = df_rejected.count()

    print(f"Total baris bersih: {total_clean}")
    print(f"Total baris ditolak: {total_rejected}")

    print("Menyimpan data bersih ke clean_parquet...")
    df_clean.write.mode("overwrite").parquet(f"file://{CLEAN_PARQUET_DIR}")

    print("Menyimpan data invalid ke rejected_parquet untuk audit...")
    df_rejected.write.mode("overwrite").parquet(f"file://{REJECTED_PARQUET_DIR}")

    spark.stop()

    print("✅ Proses cleaning selesai!")


if __name__ == "__main__":
    run_orders_analytics()
