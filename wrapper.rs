use anyhow::Ok;
use anyhow::Result;
use datafusion::arrow::array::{
    Array, ArrayAccessor, AsArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;

pub enum BatchDataFrameValue {
    Integer(i32),
    Float(f64),
    Text(String),
    Timestamp(i64),
}
pub struct BatchDataFrame {
    pub rows: Vec<HashMap<String, BatchDataFrameValue>>,
}

pub type BatchDataFrameRow = HashMap<String, BatchDataFrameValue>;
type BatchDataFrameRows = Vec<HashMap<String, BatchDataFrameValue>>;

impl BatchDataFrame {
    pub fn new(batches: Vec<RecordBatch>) -> anyhow::Result<Self> {
        // init the hashmap
        let mut rows: Vec<HashMap<String, BatchDataFrameValue>> = Vec::new();
        // the purpose of this function is to iterate the record batch and for each row, create a hashmap of the column name and the value
        for batch in batches {
            let schema = batch.schema();
            // we first check how many rows there are in the batch
            let num_rows = batch.num_rows();
            // next we check how many columns there are in the batch
            let num_cols = batch.num_columns();
            // to avoid counting the columns for each row, we do this col by col and iterate the rows by col
            for col_index in 0..num_cols {
                let column = batch.column(col_index);
                let column_name = schema.field(col_index).name();
                match column.data_type() {
                    DataType::Int32 => {
                        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        for row_index in 0..num_rows {
                            Self::update_rows_list(
                                &mut rows,
                                col_index,
                                column_name,
                                row_index,
                                BatchDataFrameValue::Integer(array.value(row_index)),
                            );
                        }
                    }
                    DataType::Utf8 => {
                        let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        for row_index in 0..num_rows {
                            Self::update_rows_list(
                                &mut rows,
                                col_index,
                                column_name,
                                row_index,
                                BatchDataFrameValue::Text(array.value(row_index).to_string()),
                            );
                        }
                    }
                    DataType::Timestamp(Microsecond, _) => {
                        let array = column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        for row_index in 0..num_rows {
                            Self::update_rows_list(
                                &mut rows,
                                col_index,
                                column_name,
                                row_index,
                                BatchDataFrameValue::Timestamp(array.value(row_index)),
                            );
                        }
                    }
                    _ => unimplemented!(),
                };
            }
        }
        Ok(Self { rows })
    }

    fn update_rows_list(
        rows: &mut BatchDataFrameRows,
        col_index: usize,
        column_name: &String,
        row_index: usize,
        value: BatchDataFrameValue,
    ) {
        if col_index == 0 {
            let mut row = HashMap::new();
            row.insert(column_name.to_string(), value);
            rows.push(row);
        } else {
            let row = rows.get_mut(row_index).unwrap();
            row.insert(column_name.to_string(), value);
        }
    }
}

impl BatchDataFrameValue {
    // Define helper methods to extract values of different types
    pub fn extract_integer(&self) -> Result<i32> {
        if let BatchDataFrameValue::Integer(val) = self {
            Ok(*val)
        } else {
            Err(anyhow::anyhow!("Value is not an integer"))
        }
    }

    pub fn extract_float(&self) -> Result<f64> {
        if let BatchDataFrameValue::Float(val) = self {
            Ok(*val)
        } else {
            Err(anyhow::anyhow!("Value is not an float"))
        }
    }

    pub fn extract_text(&self) -> Result<&str> {
        if let BatchDataFrameValue::Text(val) = self {
            Ok(val)
        } else {
            Err(anyhow::anyhow!("Value is not an float"))
        }
    }
    pub fn extract_timestamp(&self) -> Result<i64> {
        if let BatchDataFrameValue::Timestamp(val) = self {
            Ok(*val)
        } else {
            Err(anyhow::anyhow!("Value is not an i64"))
        }
    }
}
