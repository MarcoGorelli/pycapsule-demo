use arrow::array::AsArray;
use arrow::compute;
use arrow::datatypes::{DataType, Int64Type};
use arrow_schema::ArrowError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyRecordBatchReader;

#[pyfunction]
fn sum_i64_column(values: PyRecordBatchReader, column_name: String) -> PyArrowResult<i64> {
    let reader = values.into_reader()?;

    let schema = reader.schema();
    let (column_index, field) =
        schema
            .column_with_name(&column_name)
            .ok_or(ArrowError::ComputeError(format!(
                "Column '{column_name}' not found"
            )))?;

    if !matches!(field.data_type(), DataType::Int8) {
        return Err(
            ArrowError::ComputeError(format!("Expected Int64, got {}", field.data_type())).into(),
        );
    }

    // Compute sum for each chunk.
    let mut total: i64 = 0;
    for record_batch in reader {
        let record_batch = record_batch?;
        let column = record_batch.column(column_index);
        let primitive_array = column.as_primitive::<Int64Type>();
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
