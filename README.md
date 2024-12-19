# PyCapsule Demo

Demonstration of the PyCapsule Interface.

We define a function `sum_i64_column` and show that it can run on:

- Polars DataFrame
- DuckDB PyRelation
- Narwhals DataFrame (backed by Polars)

without needing PyArrow installed, and allowing us to access and
operate on the data from Rust.

This allows us to write customised, high-performance, low-level code.

The reason why it works is that all of the libraries above expose
the method `__arrow_c_stream__`. That is to say, they implement
the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).

## Running

Here are some steps you could follow if you want to run this locally.

Prerequisites: make sure you have [uv](https://github.com/astral-sh/uv) and [rust](https://rustup.rs/)
installed.

Then:
```
git clone git@github.com:MarcoGorelli/pycapsule-demo.git
cd pycapsule-demo
uv venv -p 3.12 --seed
uv pip install -U polars duckdb maturin
maturin develop  # or maturin develop --release if you want to benchmark anything
python run.py
```

