from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from eda_profile import get_numeric_columns, get_string_columns

NUMERIC_HISTOGRAM_COLUMNS = {
    "order_number",
    "days_since_prior_order",
    "add_to_cart_order",
    "reordered",
}

NUMERIC_BOXPLOT_COLUMNS = {
    "order_number",
    "days_since_prior_order",
    "add_to_cart_order",
}

def plot_null_rates(report_dir, chart_dir): 
    null_report_path = Path(report_dir) / "null_report.csv"

    if not null_report_path.exists():
        return

    df = pd.read_csv(null_report_path)
    df = df.sort_values("null_rate", ascending=True)

    plt.figure(figsize=(10, 6))
    plt.barh(df["column"], df["null_rate"])
    plt.xlabel("Null Rate")
    plt.ylabel("Column")
    plt.title("Null Rate by Column")
    plt.tight_layout()
    plt.savefig(Path(chart_dir) / "null_rate_by_column.png")
    plt.close()

def plot_numeric_distributions(df, chart_dir: str, sample_limit: int = 10000):
    numeric_cols = [
        col_name
        for col_name in get_numeric_columns(df)
        if col_name in NUMERIC_HISTOGRAM_COLUMNS
    ]

    for col_name in numeric_cols:
        sample_pdf = (
            df.select(col_name)
            .filter(F.col(col_name).isNotNull())
            .limit(sample_limit)
            .toPandas()
        )

        if sample_pdf.empty:
            continue

        plt.figure(figsize=(10, 5))
        plt.hist(sample_pdf[col_name], bins=50)
        plt.xlabel(col_name)
        plt.ylabel("Frequency")
        plt.title(f"Distribution of {col_name}")
        plt.tight_layout()
        plt.savefig(Path(chart_dir) / f"hist_{col_name}.png")
        plt.close()

        if col_name in NUMERIC_BOXPLOT_COLUMNS:
            plt.figure(figsize=(10, 4))
            plt.boxplot(sample_pdf[col_name], vert=False)
            plt.xlabel(col_name)
            plt.title(f"Boxplot of {col_name}")
            plt.tight_layout()
            plt.savefig(Path(chart_dir) / f"boxplot_{col_name}.png")
            plt.close()


def plot_top_categories(df, chart_dir: str, top_n: int = 20):
    string_cols = get_string_columns(df)

    for col_name in string_cols:
        distinct_count = df.select(col_name).distinct().count()

        if distinct_count <= 1 or distinct_count > 50:
            continue

        pdf = (
            df.groupBy(col_name)
            .count()
            .orderBy(F.desc("count"))
            .limit(top_n)
            .toPandas()
        )

        if pdf.empty:
            continue

        pdf[col_name] = pdf[col_name].astype(str)

        plt.figure(figsize=(10, 6))
        plt.barh(pdf[col_name], pdf["count"])
        plt.xlabel("Count")
        plt.ylabel(col_name)
        plt.title(f"Top Values: {col_name}")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(Path(chart_dir) / f"top_{col_name}.png")
        plt.close()


def plot_order_time_patterns(df, chart_dir: str):
    if "order_dow" in df.columns:
        pdf = (
            df.groupBy("order_dow")
            .count()
            .orderBy("order_dow")
            .toPandas()
        )

        plt.figure(figsize=(8, 5))
        plt.bar(pdf["order_dow"], pdf["count"])
        plt.xlabel("Day of Week")
        plt.ylabel("Row Count")
        plt.title("Orders/Product Rows by Day of Week")
        plt.tight_layout()
        plt.savefig(Path(chart_dir) / "orders_by_day_of_week.png")
        plt.close()

    if "order_hour_of_day" in df.columns:
        pdf = (
            df.groupBy("order_hour_of_day")
            .count()
            .orderBy("order_hour_of_day")
            .toPandas()
        )

        plt.figure(figsize=(10, 5))
        plt.bar(pdf["order_hour_of_day"], pdf["count"])
        plt.xlabel("Hour of Day")
        plt.ylabel("Row Count")
        plt.title("Orders/Product Rows by Hour of Day")
        plt.tight_layout()
        plt.savefig(Path(chart_dir) / "orders_by_hour_of_day.png")
        plt.close()
