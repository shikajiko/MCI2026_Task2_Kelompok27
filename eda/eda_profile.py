from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType


def write_schema_report(df, output_dir):
    rows = []

    for field in df.schema.fields:
        rows.append({
            "column": field.name,
            "spark_type": str(field.dataType),
            "nullable": field.nullable,
        })

    pd.DataFrame(rows).to_csv(
        Path(output_dir) / "schema.csv",
        index=False,
    )


def write_null_report(df, output_dir):
    total_rows = df.count()

    rows = []

    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()

        rows.append({
            "column": col_name,
            "null_count": null_count,
            "null_rate": null_count / total_rows if total_rows > 0 else 0,
            "non_null_count": total_rows - null_count,
        })

    pd.DataFrame(rows).sort_values(
        "null_rate",
        ascending=False,
    ).to_csv(
        Path(output_dir) / "null_report.csv",
        index=False,
    )


def write_distinct_report(df, output_dir):
    total_rows = df.count()

    rows = []

    for col_name in df.columns:
        distinct_count = df.select(col_name).distinct().count()

        rows.append({
            "column": col_name,
            "distinct_count": distinct_count,
            "distinct_rate": distinct_count / total_rows if total_rows > 0 else 0,
        })

    pd.DataFrame(rows).sort_values(
        "distinct_count",
        ascending=False,
    ).to_csv(
        Path(output_dir) / "distinct_report.csv",
        index=False,
    )


def get_numeric_columns(df):
    return [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]


def get_string_columns(df):
    return [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]


def write_numeric_report(df, output_dir):
    numeric_cols = get_numeric_columns(df)

    if not numeric_cols:
        return

    summary_df = df.select(numeric_cols).summary()
    summary_df.toPandas().to_csv(
        Path(output_dir) / "numeric_summary.csv",
        index=False,
    )

    rows = []

    for col_name in numeric_cols:
        quantiles = df.approxQuantile(
            col_name,
            [0.0, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            0.01,
        )

        rows.append({
            "column": col_name,
            "min": quantiles[0] if len(quantiles) > 0 else None,
            "p25": quantiles[1] if len(quantiles) > 1 else None,
            "p50": quantiles[2] if len(quantiles) > 2 else None,
            "p75": quantiles[3] if len(quantiles) > 3 else None,
            "p95": quantiles[4] if len(quantiles) > 4 else None,
            "p99": quantiles[5] if len(quantiles) > 5 else None,
            "max": quantiles[6] if len(quantiles) > 6 else None,
        })

    pd.DataFrame(rows).to_csv(
        Path(output_dir) / "numeric_percents.csv",
        index=False,
    )


def write_top_category_reports(df, output_dir: str, top_n: int = 20):
    category_dir = Path(output_dir) / "top_categories"
    category_dir.mkdir(parents=True, exist_ok=True)

    string_cols = get_string_columns(df)

    for col_name in string_cols:
        top_df = (
            df.groupBy(col_name)
            .count()
            .orderBy(F.desc("count"))
            .limit(top_n)
        )

        top_df.toPandas().to_csv(
            category_dir / f"{col_name}.csv",
            index=False,
        )


def write_duplicate_key_report(df, output_dir: str):
    candidate_keys = [
        col_name
        for col_name in df.columns
        if col_name == "id" or col_name.endswith("_id")
    ]

    rows = []

    for col_name in candidate_keys:
        duplicate_count = (
            df.groupBy(col_name)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        rows.append({
            "candidate_key": col_name,
            "duplicate_key_count": duplicate_count,
        })

    pd.DataFrame(rows).to_csv(
        Path(output_dir) / "duplicate_key_report.csv",
        index=False,
    )