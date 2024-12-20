from pycapsule_demo import sum_i64_column
import polars as pl
import duckdb
import narwhals as nw

df = pl.read_csv('data.csv')
df_narwhals = nw.from_native(df)
rel = duckdb.sql('select * from data.csv')

print(sum_i64_column(df, column_name='a'))
print(sum_i64_column(rel, column_name='a'))
print(sum_i64_column(df_narwhals, column_name='a'))

print(rel)

try:
    import pyarrow
except ImportError:
    print('PyArrow is not installed')
else:
    print('PyArrow is installed')
