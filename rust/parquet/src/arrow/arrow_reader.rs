use std::rc::Rc;

use arrow::datatypes::Schema;

use crate::arrow::array_reader::build_array_reader;
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::schema::parquet_to_arrow_schema_by_columns;
use crate::errors::Result;
use crate::file::reader::FileReader;
use arrow::array::Array;
use std::sync::Arc;

pub trait ArrowReader {
    fn get_schema(&mut self) -> Result<Schema>;
    fn get_schema_by_columns<T>(&mut self, column_indices: T) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>;

    /// Read row group into record batch.
    fn read_into_array<T>(&mut self, column_indices: T) -> Result<Arc<Array>>
    where
        T: IntoIterator<Item = usize>;
}

struct ParquetArrowReader {
    file_reader: Rc<FileReader>,
}

impl ArrowReader for ParquetArrowReader {
    fn get_schema(&mut self) -> Result<Schema> {
        parquet_to_arrow_schema(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
        )
    }

    fn get_schema_by_columns<T>(&mut self, column_indices: T) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>,
    {
        parquet_to_arrow_schema_by_columns(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            column_indices,
        )
    }

    fn read_into_array<T>(&mut self, column_indices: T) -> Result<Arc<Array>>
    where
        T: IntoIterator<Item = usize>,
    {
        let record_num = self.file_reader.metadata().file_metadata().num_rows();
        build_array_reader(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            column_indices,
            self.file_reader.clone(),
        )?
        .next_batch(record_num as usize)
    }
}
