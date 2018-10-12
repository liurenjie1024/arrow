use std::iter::Iterator;

use datatypes::Schema;

use error::Result;
use error::ArrowError;

use array::ArrayRef;
use record_batch::RecordBatch;

struct FileReader {
}

struct ColumnReader {
}

impl FileReader {
  pub fn column(&mut self, column_idx: usize) -> Result<ColumnReader> {
    unimplemented!()
  }

  pub fn schema(&mut self) -> Result<Schema> {
    unimplemented!()
  }

  pub fn schema_by_columns(&mut self, indices: &[usize]) -> Result<Schema> {
    unimplemented!()
  }

  pub fn read_column(&mut self, column_idx: usize) -> Result<ArrayRef> {
    unimplemented!()
  }

  pub fn record_batches(&mut self) -> impl Iterator<Item=Result<RecordBatch>> {
//    unimplemented!()
    vec![Err(ArrowError::TypeError("a".to_string()))].into_iter()
  }

  pub fn record_batches_by_columns(&mut self, column_indices: &[usize])
    -> impl Iterator<Item=Result<RecordBatch>> {
    vec![Err(ArrowError::TypeError("a".to_string()))].into_iter()
//    unimplemented!()
  }
}

