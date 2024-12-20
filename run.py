from pycapsule_demo import sum_i64_column, sum_i64_column_simple
import pandas as pd
import polars as pl
import pyarrow.csv as pa_csv
import duckdb
import narwhals as nw
from narwhals.typing import IntoFrame

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


def agnostic_sum_i64_column_rust(df_native: IntoFrame, column_name: str) -> int:
    df = nw.from_native(df_native)
    # TODO: use .collect_schema, https://github.com/narwhals-dev/narwhals/issues/1628
    schema = df.schema
    if column_name not in schema:
        msg = f"Column '{column_name}' not found, available columns are: {schema.names()}."
        raise ValueError(msg)
    if (dtype := schema[column_name]) != nw.Int64:
        msg = f"Column '{column_name}' is of type {dtype}, expected Int64"
        raise TypeError(msg)
    return sum_i64_column_simple(df, column_name)


print("***PyCapsule Interface + Narwhals solution***")
print("Polars result:", agnostic_sum_i64_column_rust(df_pl, column_name="a"))
print("pandas result:", agnostic_sum_i64_column_rust(df_pd, column_name="a"))
print("PyArrow result:", agnostic_sum_i64_column_rust(df_pa, column_name="a"))
rel = duckdb.sql("select * from data.csv")
print("DuckDB result:", agnostic_sum_i64_column_rust(rel, column_name="a"))
print()


def agnostic_sum_i64_column_python(df_native: IntoFrame, column_name: str) -> int:
    df = nw.from_native(df_native)
    schema = df.schema
    if column_name not in schema:
        msg = f"Column '{column_name}' not found, available columns are: {schema.names()}."
        raise ValueError(msg)
    if (dtype := schema[column_name]) != nw.Int64:
        msg = f"Column '{column_name}' is of type {dtype}, expected Int64"
        raise TypeError(msg)
    return df[column_name].sum()


print("***Narwhals solution***")
print("Polars result:", agnostic_sum_i64_column_python(df_pl, column_name="a"))
print("pandas result:", agnostic_sum_i64_column_python(df_pd, column_name="a"))
print("PyArrow result:", agnostic_sum_i64_column_python(df_pa, column_name="a"))
