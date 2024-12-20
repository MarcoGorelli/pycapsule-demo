use arrow::array::{AsArray, Int64Array};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow_schema::ArrowError;
use pyo3::prelude::*;
use pyo3_arrow::error::{PyArrowError, PyArrowResult};
use pyo3_arrow::PyArrayReader;

#[pyfunction]
fn sum_i64_column(values: PyArrayReader, column_name: String) -> PyArrowResult<i64> {
    let reader = values.into_reader()?;
    let field = reader.field();
    let data_type = field.data_type();
    let mut found_column = false;
    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                if field.name() == &column_name {
                    let data_type = field.data_type();
                    if data_type != &DataType::Int64 {
                        return Err(PyArrowError::from(ArrowError::ComputeError(format!(
                            "Expected Int64, got {data_type}"
                        ))));
                    }
                    found_column = true;
                    break;
                }
            }
            if !found_column {
                return Err(PyArrowError::from(ArrowError::ComputeError(format!(
                    "Column '{column_name}' not found"
                ))));
            }
        },
        data_type => {
            return Err(PyArrowError::from(ArrowError::ComputeError(format!(
                "Expected Struct, got {data_type}"
            ))));
        },
    };

    let mut total: i64 = 0;
    for array_result in reader {
        let array = array_result?;
        let struct_array = array.as_struct();
        // We use `unwrap` as we've already validated that `column_name` is present.
        let column = struct_array.column_by_name(&column_name).unwrap();
        // Data type has already been validated.
        let primitive_array: &Int64Array = column.as_primitive();
        if let Some(sum) = compute::sum(primitive_array) {
            total += sum;
        };
    }
    Ok(total)
}

#[pymodule]
fn pycapsule_demo(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_i64_column, m)?)?;
    Ok(())
}

