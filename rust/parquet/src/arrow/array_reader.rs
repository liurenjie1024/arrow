use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::mem::size_of;
use std::mem::transmute;
use std::rc::Rc;
use std::result::Result::Ok;
use std::slice::from_raw_parts;
use std::slice::from_raw_parts_mut;
use std::sync::Arc;
use std::vec::Vec;

use arrow::array::{ArrayDataBuilder, Int32BufferBuilder};
use arrow::array::ArrayDataRef;
use arrow::array::BufferBuilderTrait;
use arrow::array::StructArray;
use arrow::array::{Array, ListArray};
use arrow::array::{BooleanBufferBuilder, Int16BufferBuilder};
use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;
use arrow::buffer::MutableBuffer;
use arrow::datatypes::DataType::List;
use arrow::datatypes::{DataType as ArrowType, Field};

use crate::arrow::converter::BooleanConverter;
use crate::arrow::converter::Converter;
use crate::arrow::converter::Float32Converter;
use crate::arrow::converter::Float64Converter;
use crate::arrow::converter::Int16Converter;
use crate::arrow::converter::Int32Converter;
use crate::arrow::converter::Int64Converter;
use crate::arrow::converter::Int8Converter;
use crate::arrow::converter::UInt16Converter;
use crate::arrow::converter::UInt32Converter;
use crate::arrow::converter::UInt64Converter;
use crate::arrow::converter::UInt8Converter;
use crate::arrow::record_reader::RecordReader;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::basic::{Repetition, Type as PhysicalType};
use crate::column::page::PageIterator;
use crate::data_type::DataType;
use crate::data_type::DoubleType;
use crate::data_type::FloatType;
use crate::data_type::Int32Type;
use crate::data_type::Int64Type;
use crate::data_type::{BoolType, ByteArrayType, Int96Type};
use crate::errors::ParquetError;
use crate::errors::ParquetError::ArrowError;
use crate::errors::Result;
use crate::file::reader::{FilePageIterator, FileReader};
use crate::schema::types::{
    ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescPtr, Type, TypePtr,
};
use crate::schema::visitor::TypeVisitor;

pub trait ArrayReader {
    fn get_data_type(&self) -> &ArrowType;
    fn next_batch(&mut self, batch_size: usize) -> Result<Arc<Array>>;
    fn get_def_levels(&self) -> Option<&[i16]>;
    fn get_rep_levels(&self) -> Option<&[i16]>;
}

pub struct PrimitiveArrayReader<T: DataType> {
    data_type: ArrowType,
    pages: Box<PageIterator>,
    def_levels_buffer: Option<Buffer>,
    rep_levels_buffer: Option<Buffer>,
    null_bitmap: Option<Bitmap>,
    column_desc: ColumnDescPtr,
    record_reader: RecordReader<T>,
    _type_marker: PhantomData<T>,
}

impl<T: DataType> PrimitiveArrayReader<T> {
    pub fn new(mut pages: Box<PageIterator>, column_desc: ColumnDescPtr) -> Result<Self> {
        let data_type = parquet_to_arrow_field(column_desc.clone())?
            .data_type()
            .clone();

        let mut record_reader = RecordReader::<T>::new(column_desc.clone());
        record_reader.set_page_reader(pages.next().ok_or_else(|| {
            general_err!(
                "Can't \
                 build array without pages!"
            )
        })??)?;

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            null_bitmap: None,
            column_desc,
            record_reader,
            _type_marker: PhantomData,
        })
    }
}

impl<T: DataType> ArrayReader for PrimitiveArrayReader<T> {
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<Arc<Array>> {
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
        let arrow_type = parquet_to_arrow_field(self.column_desc.clone())?
            .data_type()
            .clone();

        let array = match (arrow_type, T::get_physical_type()) {
            (ArrowType::Boolean, PhysicalType::BOOLEAN) => unsafe {
                BooleanConverter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<BoolType>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int8, PhysicalType::INT32) => unsafe {
                Int8Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int16, PhysicalType::INT32) => unsafe {
                Int16Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int32, PhysicalType::INT32) => unsafe {
                Int32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt8, PhysicalType::INT32) => unsafe {
                UInt8Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt16, PhysicalType::INT32) => unsafe {
                UInt16Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt32, PhysicalType::INT32) => unsafe {
                UInt32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int32Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Int64, PhysicalType::INT64) => unsafe {
                Int64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::UInt64, PhysicalType::INT64) => unsafe {
                UInt64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<Int64Type>,
                >(&mut self.record_reader))
            },
            (ArrowType::Float32, PhysicalType::FLOAT) => unsafe {
                Float32Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<FloatType>,
                >(&mut self.record_reader))
            },
            (ArrowType::Float64, PhysicalType::DOUBLE) => unsafe {
                Float64Converter::convert(transmute::<
                    &mut RecordReader<T>,
                    &mut RecordReader<DoubleType>,
                >(&mut self.record_reader))
            },
            (arrow_type, _) => Err(general_err!(
                "Reading {:?} type from parquet is not supported yet.",
                arrow_type
            )),
        }?;

        //        dbg!(array.as_any().type_id());
        // save definition and repetition buffers
        self.def_levels_buffer = self.record_reader.consume_def_levels();
        self.rep_levels_buffer = self.record_reader.consume_rep_levels();
        self.null_bitmap = self.record_reader.consume_bitmap();
        Ok(array)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_ref().map(|buf| {
            let len = buf.len() * size_of::<u8>() / size_of::<i16>();
            unsafe {
                from_raw_parts(transmute::<*const u8, *const i16>(buf.raw_data()), len)
            }
        })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_ref().map(|buf| {
            let len = buf.len() * size_of::<u8>() / size_of::<i16>();
            unsafe {
                from_raw_parts(transmute::<*const u8, *const i16>(buf.raw_data()), len)
            }
        })
    }
}

struct StructArrayReader {
    children: Vec<Box<dyn ArrayReader>>,
    data_type: ArrowType,
    struct_def_level: i16,
    struct_rep_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl StructArrayReader {
    pub fn new(
        data_type: ArrowType,
        children: Vec<Box<dyn ArrayReader>>,
        def_level: i16,
        rep_level: i16,
    ) -> Self {
        Self {
            data_type,
            children,
            struct_def_level: def_level,
            struct_rep_level: rep_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for StructArrayReader {
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<Arc<Array>> {
        if self.children.len() == 0 {
            self.def_level_buffer = None;
            return Ok(Arc::new(StructArray::from(Vec::new())));
        }

        let children_array = self
            .children
            .iter_mut()
            .map(|reader| reader.next_batch(batch_size))
            .try_fold(
                Vec::new(),
                |mut result, child_array| -> Result<Vec<Arc<Array>>> {
                    result.push(child_array?);
                    Ok(result)
                },
            )?;

        // check that array child data has same size
        let children_array_len = children_array.first().unwrap().len();
        dbg!(self.get_data_type());
        dbg!(children_array_len);

        let all_children_len_eq = children_array
            .iter()
            .all(|arr| arr.len() == children_array_len);
        if !all_children_len_eq {
            return Err(general_err!("Not all children array length are the same!"));
        }

        //        let data_type = children_array.iter()
        //            .map(|arr| arr.data_ref().data_type().clone())
        //            .collect::<Vec<ArrowType>>();
        //
        //        let data_type = ArrowType::Struct(data_type);
        // calculate struct def level data
        let buffer_size = children_array_len * size_of::<i16>();
        let mut def_level_data_buffer = MutableBuffer::new(buffer_size);
        def_level_data_buffer.resize(buffer_size)?;

        let def_level_data = unsafe {
            let ptr = transmute::<*const u8, *mut i16>(def_level_data_buffer.raw_data());
            from_raw_parts_mut(ptr, buffer_size)
        };
        def_level_data
            .iter_mut()
            .for_each(|v| *v = self.struct_def_level);

        self.children.iter().try_for_each(|child| {
            if let Some(current_child_def_levels) = child.get_def_levels() {
                if current_child_def_levels.len() != children_array_len {
                    Err(general_err!("Child array length are not equal!"))
                } else {
                    for i in 0..children_array_len {
                        def_level_data[i] =
                            min(def_level_data[i], current_child_def_levels[i]);
                    }
                    Ok(())
                }
            } else {
                Ok(())
            }
        })?;

        // calculate bitmap for current array
        let mut bitmap_builder = BooleanBufferBuilder::new(children_array_len);
        let mut null_count = 0;
        def_level_data.iter().try_for_each(|item| {
            let is_null = *item < self.struct_def_level;
            if is_null {
                null_count += 1;
            }
            bitmap_builder.append(is_null)
        })?;

        let array_data = ArrayDataBuilder::new(self.data_type.clone())
            .len(children_array_len)
            .null_count(null_count)
            .null_bit_buffer(bitmap_builder.finish())
            .child_data(
                children_array
                    .iter()
                    .map(|x| x.data())
                    .collect::<Vec<ArrayDataRef>>(),
            )
            .build();

        // calculate struct rep level data, since struct doesn't add to repetition
        // levels, here we just need to keep repetition levels of first array
        // TODO: Verify that all children array reader has same repetition levels
        let rep_level_data = self
            .children
            .first()
            .unwrap()
            .get_rep_levels()
            .map(|data| -> Result<Buffer> {
                let mut buffer = Int16BufferBuilder::new(children_array_len);
                buffer.append_slice(data)?;
                Ok(buffer.finish())
            })
            .transpose()?;

        self.def_level_buffer = Some(def_level_data_buffer.freeze());
        self.rep_level_buffer = rep_level_data;
        Ok(Arc::new(StructArray::from(array_data)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer.as_ref().map(|buf| {
            let len = buf.len() * size_of::<u8>() / size_of::<i16>();
            unsafe {
                from_raw_parts(transmute::<*const u8, *const i16>(buf.raw_data()), len)
            }
        })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        // TODO: Remove these similar codes
        self.rep_level_buffer.as_ref().map(|buf| {
            let len = buf.len() * size_of::<u8>() / size_of::<i16>();
            unsafe {
                from_raw_parts(transmute::<*const u8, *const i16>(buf.raw_data()), len)
            }
        })
    }
}

struct ListArrayReader {
    data_type: ArrowType,
    child: Box<ArrayReader>,
    rep_level: i16,
    def_level: i16,
    def_level_buffer: Option<Buffer>,
    rep_level_buffer: Option<Buffer>,
}

impl ListArrayReader {
    pub fn new(child: Box<ArrayReader>, rep_level: i16, def_level: i16) -> Self {
        Self {
            data_type: List(Box::new(child.get_data_type().clone())),
            child,
            rep_level,
            def_level,
            def_level_buffer: None,
            rep_level_buffer: None,
        }
    }
}

impl ArrayReader for ListArrayReader {
    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<Arc<Array>> {
        let child_array = self.child.next_batch(batch_size)?;

        let mut offset_builder = Int32BufferBuilder::new(child_array.len());
        let mut null_bitmap_builder = BooleanBufferBuilder::new(child_array.len());
        let mut rep_level_builder = Int16BufferBuilder::new(child_array.len());
        let mut def_level_builder = Int16BufferBuilder::new(child_array.len());
        let mut len = 0;

        let arrow_type = ArrowType::List(Box::new(child_array.data_type().clone()));

        if child_array.len() > 0 {
            // Since this is a list, we assume that child array should have repetition
            // levels and definition levels
            let child_rep_levels = self.child.get_rep_levels().ok_or_else(|| {
                general_err!(
                    "Repetition levels should exist for list \
                     array!"
                )
            })?;
            let child_def_levels = self.child.get_def_levels().ok_or_else(|| {
                general_err!(
                    "Definition levels should exist for list \
                     array!"
                )
            })?;

            let mut cur_rep_level = child_rep_levels[0];
            let mut cur_def_level = child_def_levels[0];
            offset_builder.append(0)?;
            len = 1;

            for idx in 1..child_array.len() {
                if child_rep_levels[idx] != self.rep_level {
                    len = len + 1;
                    offset_builder.append(idx as i32)?;
                    rep_level_builder.append(cur_rep_level)?;
                    def_level_builder.append(cur_def_level)?;

                    null_bitmap_builder.append(cur_def_level >= self.def_level)?;
                } else {
                    cur_rep_level = min(cur_rep_level, child_rep_levels[idx]);
                    cur_def_level = min(cur_def_level, child_def_levels[idx]);
                }
            }

            null_bitmap_builder.append(cur_def_level >= self.def_level)?;
            rep_level_builder.append(cur_rep_level)?;
            def_level_builder.append(cur_def_level)?;
        }

        self.rep_level_buffer = Some(rep_level_builder.finish());
        dbg!(self.rep_level_buffer.as_ref().map(|x| x.len()).unwrap());
        self.def_level_buffer = Some(def_level_builder.finish());
        dbg!(self.def_level_buffer.as_ref().map(|x| x.len()).unwrap());

        let offset_buffer = offset_builder.finish();
        dbg!(offset_buffer.typed_data::<i32>());
        let null_bitmap = null_bitmap_builder.finish();

        let array_data = ArrayDataBuilder::new(arrow_type)
            .len(dbg!(len))
            .add_buffer(offset_buffer)
            .null_bit_buffer(null_bitmap)
            .add_child_data(child_array.data())
            .build();

        Ok(Arc::new(ListArray::from(array_data)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_level_buffer.as_ref().map(|buf| {
            let len = buf.len() * size_of::<u8>() / size_of::<i16>();
            unsafe {
                from_raw_parts(transmute::<*const u8, *const i16>(buf.raw_data()), len)
            }
        })
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_level_buffer.as_ref().map(|buf| {
            let len = buf.len() * size_of::<u8>() / size_of::<i16>();
            unsafe {
                from_raw_parts(transmute::<*const u8, *const i16>(buf.raw_data()), len)
            }
        })
    }
}

pub fn build_array_reader<T>(
    parquet_schema: SchemaDescPtr,
    column_indices: T,
    file_reader: Rc<FileReader>,
) -> Result<Box<ArrayReader>>
where
    T: IntoIterator<Item = usize>,
{
    let mut base_nodes = Vec::new();
    let mut base_nodes_set = HashSet::new();
    let mut leaves = HashMap::<*const Type, usize>::new();

    for c in column_indices {
        let column = parquet_schema.column(c).self_type() as *const Type;
        let root = parquet_schema.get_column_root_ptr(c);
        let root_raw_ptr = root.clone().as_ref() as *const Type;

        leaves.insert(column, c);
        if !base_nodes_set.contains(&root_raw_ptr) {
            base_nodes.push(root);
            base_nodes_set.insert(root_raw_ptr);
        }
    }

    if leaves.is_empty() {
        return Err(general_err!("Can't build array reader without columns!"));
    }

    ArrayReaderBuilder::new(
        Rc::new(parquet_schema.root_schema().clone()),
        Rc::new(leaves),
        file_reader,
    )
    .build_array_reader()
}

struct ArrayReaderBuilder {
    root_schema: TypePtr,
    // Key: columns that need to be included in final array builder
    // Value: column index in schema
    columns_included: Rc<HashMap<*const Type, usize>>,
    file_reader: Rc<FileReader>,
}

#[derive(Clone)]
struct ArrayReaderBuilderContext {
    def_level: i16,
    rep_level: i16,
    path: ColumnPath,
}

impl Default for ArrayReaderBuilderContext {
    fn default() -> Self {
        Self {
            def_level: 0i16,
            rep_level: 0i16,
            path: ColumnPath::new(Vec::new()),
        }
    }
}

impl<'a> TypeVisitor<Option<Box<ArrayReader>>, &'a ArrayReaderBuilderContext>
    for ArrayReaderBuilder
{
    /// Build array reader for primitive type.
    fn visit_primitive(
        &mut self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<ArrayReader>>> {
        if self.is_included(cur_type.as_ref()) {
            let mut new_context = context.clone();
            new_context.path.append(vec![cur_type.name().to_string()]);

            match cur_type.get_basic_info().repetition() {
                Repetition::REPEATED => {
                    new_context.def_level += 1;
                    new_context.rep_level += 1;
                }
                Repetition::OPTIONAL => {
                    new_context.def_level += 1;
                }
                _ => (),
            }

            let reader =
                self.build_for_primitive_type_inner(cur_type.clone(), &new_context)?;

            if cur_type.get_basic_info().repetition() == Repetition::REPEATED {
                Ok(Some(Box::new(ListArrayReader::new(
                    reader,
                    new_context.rep_level,
                    new_context.def_level,
                ))))
            } else {
                Ok(Some(reader))
            }
        } else {
            Ok(None)
        }
    }

    fn visit_struct(
        &mut self,
        cur_type: Rc<Type>,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<ArrayReader>>> {

        let mut new_context = context.clone();
        new_context.path.append(vec![cur_type.name().to_string()]);


        if cur_type.get_basic_info().has_repetition() {
            match cur_type.get_basic_info().repetition() {
                Repetition::REPEATED => {
                    new_context.def_level += 1;
                    new_context.rep_level += 1;
                }
                Repetition::OPTIONAL => {
                    new_context.def_level += 1;
                }
                _ => (),
            }
        }

        if let Some(reader) =
            self.build_for_struct_type_inner(cur_type.clone(), &new_context)?
        {
            if cur_type.get_basic_info().has_repetition() && cur_type.get_basic_info()
            .repetition() ==
                Repetition::REPEATED {
                Ok(Some(Box::new(ListArrayReader::new(
                    reader,
                    new_context.rep_level,
                    new_context.def_level,
                ))))
            } else {
                Ok(Some(reader))
            }
        } else {
            Ok(None)
        }
    }

    fn visit_map(
        &mut self,
        _cur_type: Rc<Type>,
        _context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<ArrayReader>>> {
        Err(ArrowError(format!(
            "Reading parquet map array into arrow is not supported yet!"
        )))
    }

    fn visit_list_with_item(
        &mut self,
        list_type: Rc<Type>,
        item_type: &Type,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<ArrayReader>>> {
        let mut new_context = context.clone();
        new_context.rep_level += 1;
        new_context.def_level += 1;
        new_context.path.append(vec![list_type.name().to_string()]);

        self.dispatch(Rc::new(item_type.clone()), &new_context)
            .map(|child_opt| {
                child_opt.map(|child| -> Box<dyn ArrayReader> {
                    Box::new(ListArrayReader::new(
                        child,
                        new_context.rep_level,
                        new_context.def_level,
                    ))
                })
            })
    }
}

impl<'a> ArrayReaderBuilder {
    fn new(
        root_schema: TypePtr,
        columns_included: Rc<HashMap<*const Type, usize>>,
        file_reader: Rc<FileReader>,
    ) -> Self {
        Self {
            root_schema,
            columns_included,
            file_reader,
        }
    }

    fn build_array_reader(&mut self) -> Result<Box<ArrayReader>> {
        let context = ArrayReaderBuilderContext::default();

        self.visit_struct(self.root_schema.clone(), &context)
            .map(|reader| reader.unwrap())
    }

    // Utility functions
    fn is_included(&self, t: &Type) -> bool {
        self.columns_included.contains_key(&(t as *const Type))
    }

    // Functions for primitive types.
    fn build_for_primitive_type_inner(
        &self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Box<ArrayReader>> {
        let column_desc = Rc::new(ColumnDescriptor::new(
            cur_type.clone(),
            Some(self.root_schema.clone()),
            context.def_level,
            context.rep_level,
            context.path.clone(),
        ));
        let page_iterator = Box::new(FilePageIterator::new(
            self.columns_included[&(cur_type.as_ref() as *const Type)],
            self.file_reader.clone(),
        )?);

        match cur_type.get_physical_type() {
            PhysicalType::BOOLEAN => Ok(Box::new(PrimitiveArrayReader::<BoolType>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::INT32 => Ok(Box::new(PrimitiveArrayReader::<Int32Type>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::INT64 => Ok(Box::new(PrimitiveArrayReader::<Int64Type>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::INT96 => Ok(Box::new(PrimitiveArrayReader::<Int96Type>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::FLOAT => Ok(Box::new(PrimitiveArrayReader::<FloatType>::new(
                page_iterator,
                column_desc,
            )?)),
            PhysicalType::DOUBLE => Ok(Box::new(
                PrimitiveArrayReader::<DoubleType>::new(page_iterator, column_desc)?,
            )),
            PhysicalType::BYTE_ARRAY => Ok(Box::new(PrimitiveArrayReader::<
                ByteArrayType,
            >::new(
                page_iterator, column_desc
            )?)),
            other => Err(ArrowError(format!(
                "Unable to create primite array reader for parquet physical type {}",
                other
            ))),
        }
    }

    fn build_for_struct_type_inner(
        &mut self,
        cur_type: TypePtr,
        context: &'a ArrayReaderBuilderContext,
    ) -> Result<Option<Box<ArrayReader>>> {
        let mut fields = Vec::with_capacity(cur_type.get_fields().len());
        let mut children_reader = Vec::with_capacity(cur_type.get_fields().len());

        for child in cur_type.get_fields() {
            if let Some(child_reader) = self.dispatch(child.clone(), context)? {
                fields.push(Field::new(
                    child.name(),
                    child_reader.get_data_type().clone(),
                    child.is_optional(),
                ));
                children_reader.push(child_reader);
            }
        }

        if !fields.is_empty() {
            let arrow_type = ArrowType::Struct(fields);
            Ok(Some(Box::new(StructArrayReader::new(
                arrow_type,
                children_reader,
                context.def_level,
                context.rep_level,
            ))))
        } else {
            Ok(None)
        }
    }
}
