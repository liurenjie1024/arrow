use std::sync::Arc;
use arrow::array::Array;
use super::page_iterator::PageIterator;
use super::record_reader::RecordReader;

use crate::errors::Result;
use crate::data_type::DataType;

pub trait ArrayReader {
    fn next_batch(&mut self, batch_size: usize) -> Option<Result<Arc<Array>>>;
}

struct PrimitiveArrayReader<T: DataType> {
    pages: Box<PageIterator>,
    record_reader: RecordReader<T>,
}

impl<T: DataType> ArrayReader for PrimitiveArrayReader<T> {
    fn next_batch(&mut self, batch_size: usize) -> Option<Result<Arc<Array>>> {
        self.record_reader.reset();

        let mut records_read = 0usize;
        while records_read < batch_size {
            let records_to_read = batch_size - records_read;

            let records_read_once = self.record_reader.read_records(records_to_read)?;
            records_read = records_read + records_read_once;

            // Record reader exhausted
            if records_read_once < records_to_read {
                if let Some(page_reader) = self.pages.next() {
                    // Read from new page reader
                    self.record_reader.set_page_reader(page_reader?);
                } else {
                    // Page reader also exhausted
                    break;
                }
            }
        }

        // Begin to convert to arrays

    }
}