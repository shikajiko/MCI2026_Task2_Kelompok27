from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from eda_profile import get_numeric_columns, get_string_columns

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
    plt.savefig(Path(char_dir) / "null_rate_by_column.png")
    plt.close()
