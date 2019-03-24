use std::sync::Arc;
use std::vec::Vec;
use std::mem::transmute;
use arrow::array::Array;
use arrow::datatypes::DataType as ArrowType;
use super::page_iterator::PageIterator;

use crate::errors::Result;
use crate::data_type::DataType;
use crate::basic::Type;
use crate::data_type::BoolType;
use crate::data_type::Int32Type;
use crate::data_type::Int64Type;
use crate::data_type::FloatType;
use crate::data_type::DoubleType;
use crate::reader::converter::Converter;
use crate::reader::converter::BooleanConverter;
use crate::reader::record_reader::RecordReader;
use crate::errors::ParquetError;
use crate::reader::converter::Int8Converter;
use crate::reader::converter::Int16Converter;
use crate::reader::converter::Int32Converter;
use crate::reader::converter::UInt8Converter;
use crate::reader::converter::UInt16Converter;
use crate::reader::converter::UInt32Converter;
use crate::reader::converter::Int64Converter;
use crate::reader::converter::UInt64Converter;
use crate::reader::converter::Float32Converter;
use crate::reader::converter::Float64Converter;


pub trait ArrayReader {
    fn next_batch(&mut self, batch_size: usize) -> Result<Option<Arc<Array>>>;
}


struct PrimitiveArrayReader<T: DataType> {
    pages: Box<PageIterator>,
    record_reader: RecordReader<T>,
    column_type: ArrowType,
    leaf_type: ArrowType,
}

impl<T: DataType> ArrayReader for PrimitiveArrayReader<T> {
    fn next_batch(&mut self, batch_size: usize) -> Result<Option<Arc<Array>>> {
        self.record_reader.reset()?;

        let mut records_read = 0usize;
        while records_read < batch_size {
            let records_to_read = batch_size - records_read;

            let records_read_once = self.record_reader.read_records(records_to_read)?;
            records_read = records_read + records_read_once;

            // Record reader exhausted
            if records_read_once < records_to_read {
                if let Some(page_reader) = self.pages.next() {
                    // Read from new page reader
                    self.record_reader.set_page_reader(page_reader?)?;
                } else {
                    // Page reader also exhausted
                    break;
                }
            }
        }

        // Begin to convert to arrays
        let array = match (self.leaf_type.clone(), T::get_physical_type()) {
            (ArrowType::Boolean, Type::BOOLEAN) => unsafe {
                BooleanConverter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<BoolType>>(&mut self.record_reader))
            },
            (ArrowType::Int8, Type::INT32) => unsafe {
                 Int8Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int32Type>>(&mut self.record_reader))
            },
            (ArrowType::Int16, Type::INT32) => unsafe {
                Int16Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int32Type>>(&mut self.record_reader))
            },
            (ArrowType::Int32, Type::INT32) => unsafe {
                Int32Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int32Type>>(&mut self.record_reader))
            },
            (ArrowType::UInt8, Type::INT32) => unsafe {
                UInt8Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int32Type>>(&mut self.record_reader))
            },
            (ArrowType::UInt16, Type::INT32) => unsafe {
                UInt16Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int32Type>>(&mut self.record_reader))
            },
            (ArrowType::UInt32, Type::INT32) => unsafe {
                UInt32Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int32Type>>(&mut self.record_reader))
            },
            (ArrowType::Int64, Type::INT64) => unsafe {
                Int64Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int64Type>>(&mut self.record_reader))
            },
            (ArrowType::UInt64, Type::INT64) => unsafe {
                UInt64Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<Int64Type>>(&mut self.record_reader))
            },
            (ArrowType::Float32, Type::FLOAT) => unsafe {
                Float32Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<FloatType>>(&mut self.record_reader))
            },
            (ArrowType::Float64, Type::DOUBLE) => unsafe {
                Float64Converter::convert(transmute::<&mut RecordReader<T>,
                    &mut RecordReader<DoubleType>>(&mut self.record_reader))
            },
            (arrow_type, _) => Err(general_err!(
            "Reading {:?} type from parquet is not supported yet.", arrow_type))
        }?;
        Ok(Some(array))
    }
}

struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    def_level: i16
}

impl ArrayReader for StructArrayReader {
    fn next_batch(&mut self, batch_size: usize) -> Result<Option<Arc<Array>>> {
        unimplemented!()
    }
}
