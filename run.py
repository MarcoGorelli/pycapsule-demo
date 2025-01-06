import duckdb
import narwhals as nw
import pandas as pd
import polars as pl
import pyarrow.csv as pa_csv
from narwhals.typing import IntoFrame
from pycapsule_demo import sum_i64_column

df_pl = pl.read_csv("data.csv")
df_pd = pd.read_csv("data.csv")
df_pa = pa_csv.read_csv("data.csv")
rel = duckdb.sql("select * from data.csv")

print("***PyCapsule Interface solution***")
print("Polars result:", sum_i64_column(df_pl, column_name="a"))
print("pandas result:", sum_i64_column(df_pd, column_name="a"))
print("PyArrow result:", sum_i64_column(df_pa, column_name="a"))
print("DuckDB result:", sum_i64_column(rel, column_name="a"))
print()


def agnostic_sum_i64_column_narwhals(df_native: IntoFrame, column_name: str) -> int:
    lf = nw.from_native(df_native).lazy()
    schema = lf.collect_schema()
    if column_name not in schema:
        msg = f"Column '{column_name}' not found, available columns are: {schema.names()}."
        raise ValueError(msg)
    if (dtype := schema[column_name]) != nw.Int64:
        msg = f"Column '{column_name}' is of type {dtype}, expected Int64"
        raise TypeError(msg)
    df = lf.collect()
    return df[column_name].sum()


print("***Narwhals solution***")
print("Polars result:", agnostic_sum_i64_column_narwhals(df_pl, column_name="a"))
print("pandas result:", agnostic_sum_i64_column_narwhals(df_pd, column_name="a"))
print("PyArrow result:", agnostic_sum_i64_column_narwhals(df_pa, column_name="a"))
# Needs re-executing due to https://github.com/duckdb/duckdb/discussions/15536.
rel = duckdb.sql("select * from data.csv")
print("DuckDB result:", agnostic_sum_i64_column_narwhals(rel, column_name="a"))
