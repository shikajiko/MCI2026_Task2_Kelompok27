import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from eda_profile import (
    write_schema_report,
    write_null_report,
    write_distinct_report,
    write_numeric_report,
    write_top_category_reports,
)
from eda_charts import (
    plot_null_rates,
    plot_numeric_distributions,
    plot_top_categories,
    plot_order_time_patterns,
)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        default="/opt/airflow/data_lake/orders/raw_parquet",
    )

    parser.add_argument(
        "--output-dir",
        default="/opt/airflow/data_lake/orders/eda",
    )

    return parser.parse_args()

def main():
    args = parse_args()

    report_dir = Path(args.output_dir) / "reports"
    chart_dir = Path(args.output_dir) / "charts"

    report_dir.mkdir(parents=True, exist_ok=True)
    chart_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Orders_EDA")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Membaca raw Parquet pada: {args.input_dir}")
    df = spark.read.parquet(f"file://{args.input_dir}")

    print("Schema:")
    df.printSchema()

    print("Membuat eda reports...")
    write_schema_report(df, str(report_dir))
    write_null_report(df, str(report_dir))
    write_distinct_report(df, str(report_dir))
    write_numeric_report(df, str(report_dir))
    write_top_category_reports(df, str(report_dir))

    print("Membuat charts...")
    plot_null_rates(str(report_dir), str(chart_dir))
    plot_numeric_distributions(df, str(chart_dir))
    plot_top_categories(df, str(chart_dir))
    plot_order_time_patterns(df, str(chart_dir))

    print(f"Reports: {report_dir}")
    print(f"Charts: {chart_dir}")

    spark.stop()

if __name__ == "__main__":
    main()
