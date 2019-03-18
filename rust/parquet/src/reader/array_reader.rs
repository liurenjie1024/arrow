use std::sync::Arc;
use std::mem::transmute;
use arrow::array::Array;
use arrow::builder::PrimitiveBuilder;
use arrow::datatypes::DataType as ArrowType;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::*;
use super::page_iterator::PageIterator;
use super::record_reader::RecordReader;

use crate::errors::Result;
use crate::data_type::DataType;
use crate::basic::Type;
use arrow::builder::BooleanBuilder;
use arrow::builder::BooleanBuilder;
use std::slice::from_raw_parts;


pub trait ArrayReader {
    fn next_batch(&mut self, batch_size: usize) -> Option<Result<Arc<Array>>>;
}


struct PrimitiveArrayReader<T: DataType> {
    pages: Box<PageIterator>,
    record_reader: RecordReader<T>,
    column_type: ArrowType,
    leaf_type: ArrowType,
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
        let records_data = self.record_reader.records_data()?;
        let def_levels = self.record_reader.definition_levels_data();

        let array = match (self.leaf_type, T::get_physical_type()) {
            (ArrowType::Boolean, Type::BOOLEAN) =>
                parquet_to_arrow_primitive::<BooleanType>(records_read, records_data,
                                                          def_levels),
            (ArrowType::Int8, Type::INT32) =>
                parquet_to_arrow_primitive::<Int8Type>(records_read, records_data,
                                                       def_levels),
            (ArrowType::Int16, Type::INT32) =>
                parquet_to_arrow_primitive::<Int16Type>(records_read, records_data,
                                                        def_levels),
            (ArrowType::Int32, Type::INT32) =>
                parquet_to_arrow_primitive::<Int32Type>(records_read, records_data,
                                                        def_levels),
            (ArrowType::Int64, Type::INT64) =>
                parquet_to_arrow_primitive::<Int64Type>(records_read, records_data,
                                                        def_levels),
            (ArrowType::UInt8, Type::INT32) =>
                parquet_to_arrow_primitive::<UInt8Type>(records_read, records_data,
                                                        def_levels),
            (ArrowType::UInt16, Type::INT32) =>
                parquet_to_arrow_primitive::<UInt16Type>(records_read, records_data,
                                                         def_levels),

            (ArrowType::UInt32, Type::INT32) =>
                parquet_to_arrow_primitive::<UInt32Type>(records_read, records_data,
                                                         def_levels),
            (ArrowType::UInt64, Type::INT64) =>
                parquet_to_arrow_primitive::<UInt64Type>(records_read, records_data,
                                                         def_levels),

            (ArrowType::Float32, Type::FLOAT) =>
                parquet_to_arrow_primitive::<Float32Type>(records_read, records_data,
                                                          def_levels),
            (ArrowType::Float64, Type::DOUBLE) =>
                parquet_to_arrow_primitive::<Float64Type>(records_read, records_data,
                                                          def_levels),
            (arrow_type, _) => Err(general_err!(
            "Reading {:?} type from parquet is not supported yet.", arrow_type))
        }?;
    }
}


fn parquet_to_arrow_primitive<T: ArrowPrimitiveType, P: DataType>(size: usize, data: &[u8],
                         def_levels: Option<&[i16]>) -> Result<Arc<Array>> {
    //TODO: Optimize this to use mutable buffer directly
    let data_ptr = unsafe {
        transmute::<*const u8, *const P::T>(data.as_ptr())
    };
    let data_slices = unsafe {
        from_raw_parts(data_ptr, size)
    };



    let mut builder = PrimitiveBuidler::<T>::new(size);

    match def_levels {
        Some(null_values) => {
            data_slices.iter().zip(null_values.iter())
                .for_each(|(data, is_null)| {
                    if *is_null > 0 {
                        builder.append_null()
                    } else {
                        builder.append_value(data as T::Native)
                    }
                })
        }

        None => {
            data_slices.iter()
                .for_each(|v| builder.append_value(v as T::Native))
        }
    };

    builder.finish()
}

