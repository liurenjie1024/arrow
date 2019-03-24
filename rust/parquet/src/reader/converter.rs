use super::record_reader::RecordReader;
use crate::data_type::DataType;
use arrow::array::Array;
use std::sync::Arc;
use std::convert::From;

use crate::errors::Result;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::builder::BufferBuilder;
use arrow::builder::BufferBuilderTrait;

use arrow::array_data::ArrayDataBuilder;
use arrow::array::PrimitiveArray;
use std::mem::transmute;
use std::marker::PhantomData;

use crate::data_type::BoolType;
use crate::data_type::Int32Type as ParquetInt32Type;
use crate::data_type::Int64Type as ParquetInt64Type;
use crate::data_type::FloatType as ParquetFloatType;
use crate::data_type::DoubleType as ParquetDoubleType;
use arrow::datatypes::BooleanType;
use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::UInt64Type;
use arrow::datatypes::Float32Type;
use arrow::datatypes::Float64Type;


pub trait Converter<T: DataType> {
    fn convert(record_reader: &mut RecordReader<T>) -> Result<Arc<Array>>;
}

pub trait ConvertAs<T> {
    fn convert_as(self) -> T;
}

impl<T> ConvertAs<T> for T {
    fn convert_as(self) -> T {
        self
    }
}

macro_rules! convert_as {
    ($src_type: ty, $dest_type: ty) => {
        impl ConvertAs<$dest_type> for $src_type {
            fn convert_as(self) -> $dest_type {
                self as $dest_type
            }
        }
    };
}

convert_as!(i32, i8);
convert_as!(i32, i16);
convert_as!(i32, u8);
convert_as!(i32, u16);
convert_as!(i32, u32);


pub struct BuilderConverter<ParquetType, ArrowType>
{
    _parquet_marker: PhantomData<ParquetType>,
    _arrow_marker:   PhantomData<ArrowType>
}

impl<ParquetType, ArrowType> Converter<ParquetType> for BuilderConverter<ParquetType, ArrowType>
    where ParquetType: DataType,
          ArrowType: ArrowPrimitiveType,
          <ParquetType as DataType>::T : ConvertAs<<ArrowType as ArrowPrimitiveType>::Native>
{
    fn convert(record_reader: &mut RecordReader<ParquetType>) -> Result<Arc<Array>> {
        let values_num = record_reader.values_num();
        let mut builder = BufferBuilder::<ArrowType>::new(record_reader.values_num());

        let data: Vec<<ArrowType as ArrowPrimitiveType>::Native> = unsafe {
            let records_data = record_reader.consume_data();
            let data_ptr = transmute::<*const u8, *mut ParquetType::T>(
                records_data.raw_data());
            let data = Vec::from_raw_parts(data_ptr, values_num, values_num)
                .into_iter()
                .map(|e|e.convert_as())
                .collect();
            std::mem::drop(records_data);
            data
        };


        builder.append_slice(data.as_slice())?;

        let mut array_data = ArrayDataBuilder::new(ArrowType::get_data_type())
            .add_buffer(builder.finish());

        if let Some(b) =  record_reader.consume_null_bit_buffer() {
            array_data = array_data.null_bit_buffer(b);
        }

        Ok(Arc::new(PrimitiveArray::<ArrowType>::from(array_data.build())))
    }
}

pub struct DirectConverter<ParquetType, ArrowType> {
    _parquet_marker: PhantomData<ParquetType>,
    _arrow_marker:   PhantomData<ArrowType>
}

impl<ParquetType, ArrowType> Converter<ParquetType> for
    DirectConverter<ParquetType, ArrowType>
where ParquetType: DataType,
      ArrowType: ArrowPrimitiveType,
      {
    fn convert(record_reader: &mut RecordReader<ParquetType>) -> Result<Arc<Array>> {
        let mut array_data = ArrayDataBuilder::new(ArrowType::get_data_type())
            .add_buffer(record_reader.consume_data());

        if let Some(b) =  record_reader.consume_null_bit_buffer() {
            array_data = array_data.null_bit_buffer(b);
        }

        Ok(Arc::new(PrimitiveArray::<ArrowType>::from(array_data.build())))
    }
}

pub type BooleanConverter = DirectConverter<BoolType, BooleanType>;
pub type Int8Converter = BuilderConverter<ParquetInt32Type, Int8Type>;
pub type UInt8Converter = BuilderConverter<ParquetInt32Type, UInt8Type>;
pub type Int16Converter = BuilderConverter<ParquetInt32Type, Int16Type>;
pub type UInt16Converter = BuilderConverter<ParquetInt32Type, UInt16Type>;
pub type Int32Converter = DirectConverter<ParquetInt32Type, Int32Type>;
pub type UInt32Converter = DirectConverter<ParquetInt32Type, UInt32Type>;
pub type Int64Converter = DirectConverter<ParquetInt64Type, Int64Type>;
pub type UInt64Converter = DirectConverter<ParquetInt64Type, UInt64Type>;
pub type Float32Converter = DirectConverter<ParquetFloatType, Float32Type>;
pub type Float64Converter = DirectConverter<ParquetDoubleType, Float64Type>;




//macro_rules! make_builder_converter {
//    ($name: ident, $parquet_type: ty, $physical_type: ty, $native_type: ty, $builder: ty)
//    => {
//        struct $name {}
//
//        impl Converter<$parquet_type> for $name {
//            fn convert(record_reader: &mut RecordReader<$parquet_type>) ->
//            Result<Arc<Array>> {
//                let values_num = record_reader.values_num();
//                let builder = $builder::new(values_num);
//
//                let data = unsafe {
//                    let records_data = self.record_reader.consume_data();
//                    let data_ptr = transmute::<*const u8, *const $physical_type>(
//                        data.raw_data());
//                    from_raw_parts(data_ptr, values_num);
//                }
//
//                let bitmap = record_reader.consume_bitmap();
//                match bitmap {
//                    Some(b) => {
//                        for idx in 0..bitmap.len() {
//                            if bitmap.is_set(idx) {
//                                builder.append(data[idx] as $native_type)
//                            } else {
//                                builder.append()
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//}


