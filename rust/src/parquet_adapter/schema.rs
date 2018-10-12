use std::collections::HashSet;
use std::rc::Rc;

use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::ColumnDescPtr;
use parquet::schema::types::Type;
use parquet::schema::types::TypePtr;
use parquet::schema::types::BasicTypeInfo;
use parquet::basic::LogicalType;
use parquet::basic::Type as PhysicalType;
use parquet::basic::Repetition;

use datatypes::Schema;
use datatypes::Field;
use datatypes::DataType;

use error::ArrowError;
use error::Result;

pub fn parquet_to_arrow_schema(parquet_schema: SchemaDescPtr) -> Result<Schema> {
  parquet_to_arrow_schema_by_columns(parquet_schema.clone(), 0..parquet_schema.columns().len())
}

pub fn parquet_to_arrow_schema_by_columns<T>(
  parquet_schema: SchemaDescPtr, column_indices: T)
  -> Result<Schema>
  where T: IntoIterator<Item=usize> {
  let leaves = column_indices
    .into_iter()
    .map(|c| parquet_schema.column(c).self_type() as *const Type)
    .collect::<HashSet<*const Type>>();

  let fields = parquet_schema.root_schema().get_fields()
    .iter()
    .map(|t| parquet_to_arrow_field(t.clone(), &leaves))
    .collect::<Result<Vec<Option<Field>>>>()?
    .into_iter()
    .filter_map(|f| f)
    .collect::<Vec<Field>>();

  Ok(Schema::new(fields))
}

fn parquet_to_arrow_field(schema: TypePtr, leaves: &HashSet<*const Type>) -> Result<Option<Field>> {
  match &*schema {
    Type::PrimitiveType {..} => parquet_to_arrow_primitive_field(&*schema, &leaves),
    Type::GroupType {..} => parquet_to_arrow_struct_field(&*schema, &leaves)
  }
}

fn parquet_to_arrow_primitive_field(schema: &Type, leaves: &HashSet<*const Type>)
  -> Result<Option<Field>> {
  match schema {
    Type::PrimitiveType {
      basic_info,
      physical_type,
      ..
    } => {
      let raw_ptr = schema as *const Type;
      if leaves.contains(&raw_ptr) {
        let data_type = match physical_type {
          PhysicalType::BOOLEAN => Ok(DataType::Boolean),
          PhysicalType::INT32 => parquet_to_arrow_int32(&basic_info),
          PhysicalType::INT64 => parquet_to_arrow_int64(&basic_info),
          PhysicalType::FLOAT => Ok(DataType::Float32),
          PhysicalType::DOUBLE => Ok(DataType::Float64),
          PhysicalType::BYTE_ARRAY => parquet_to_arrow_byte_array(&basic_info),
          other => Err(ArrowError::TypeError(format!("Unable to convert parquet type {}", other)))
        };

        data_type.map(|dt| {
          match basic_info.repetition() {
            Repetition::REQUIRED => Field::new(basic_info.name(), dt, false),
            Repetition::OPTIONAL => Field::new(basic_info.name(), dt, true),
            Repetition::REPEATED => Field::new(basic_info.name(),
                                               DataType::List(Box::new(dt)), true)
          }
        }).map(|f| Some(f))
      } else {
        Ok(None)
      }
    },
    Type::GroupType {..} => panic!("This should not happen.")
  }
}

fn parquet_to_arrow_struct_field(schema: &Type, leaves: &HashSet<*const Type>)
  -> Result<Option<Field>> {
  match schema {
    Type::PrimitiveType {..} => panic!("This should not happen."),
    Type::GroupType {
      basic_info,
      fields
    } => {
      let struct_fields = fields.iter()
        .map(|field_ptr| parquet_to_arrow_field(field_ptr.clone(), leaves))
        .collect::<Result<Vec<Option<Field>>>>()?
        .into_iter()
        .filter_map(|f| f)
        .collect::<Vec<Field>>();

      if struct_fields.is_empty() {
        Ok(None)
      } else {
        let basic_data_type = DataType::Struct(struct_fields);

        let field = if basic_info.has_repetition() {
          match basic_info.repetition() {
            Repetition::REPEATED => Field::new(basic_info.name(),
                                               DataType::List(Box::new(basic_data_type)),
                                               true),
            Repetition::REQUIRED => Field::new(basic_info.name(), basic_data_type, false),
            Repetition::OPTIONAL => Field::new(basic_info.name(), basic_data_type, true),
          }
        } else {
          Field::new(basic_info.name(), basic_data_type, true)
        };

        Ok(Some(field))
      }
    }
  }
}

fn parquet_to_arrow_int32(t: &BasicTypeInfo) -> Result<DataType> {
  match t.logical_type() {
    LogicalType::NONE => Ok(DataType::Int32),
    LogicalType::UINT_8 => Ok(DataType::UInt8),
    LogicalType::UINT_16 => Ok(DataType::UInt16),
    LogicalType::UINT_32 => Ok(DataType::UInt32),
    LogicalType::INT_8 => Ok(DataType::Int8),
    LogicalType::INT_16 => Ok(DataType::Int16),
    LogicalType::INT_32 => Ok(DataType::Int32),
    other => Err(ArrowError::TypeError(format!("Unable to convert parquet logical type {}", other)))
  }
}

fn parquet_to_arrow_int64(t: &BasicTypeInfo) -> Result<DataType> {
  match t.logical_type() {
    LogicalType::NONE => Ok(DataType::Int64),
    LogicalType::INT_64 => Ok(DataType::Int64),
    LogicalType::UINT_64 => Ok(DataType::UInt64),
    other => Err(ArrowError::TypeError(format!("Unable to convert parquet logical type {}", other)))
  }
}

fn parquet_to_arrow_byte_array(t: &BasicTypeInfo) -> Result<DataType> {
  match t.logical_type() {
    LogicalType::UTF8 => Ok(DataType::Utf8),
    other => Err(ArrowError::TypeError(format!("Unable to convert parquet logical type {}", other)))
  }
}

#[cfg(test)]
mod tests {
  use std::rc::Rc;

  use parquet::schema::types::GroupTypeBuilder;
  use parquet::schema::types::SchemaDescriptor;
  use parquet::basic::Type as PhysicalType;
  use parquet::basic::LogicalType;
  use parquet::basic::Repetition;
  use parquet::schema::types::PrimitiveTypeBuilder;

  use datatypes::Field;
  use datatypes::DataType;
  use datatypes::Schema;

  use super::parquet_to_arrow_schema;
  use super::parquet_to_arrow_schema_by_columns;

  macro_rules! make_parquet_type {
    ($name: expr, $physical_type: expr,  $($attr: ident : $value: expr),*) => {{
      let mut builder = PrimitiveTypeBuilder::new($name, $physical_type);

      $(
        builder = builder.$attr($value);
      )*

      Rc::new(builder.build().unwrap())
    }}
  }

  #[test]
  fn test_flat_primitives() {
    let mut parquet_types = vec![
      make_parquet_type!("boolean", PhysicalType::BOOLEAN, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("int8", PhysicalType::INT32, with_repetition: Repetition::REQUIRED,
        with_logical_type: LogicalType::INT_8),
      make_parquet_type!("int16", PhysicalType::INT32, with_repetition: Repetition::REQUIRED,
        with_logical_type: LogicalType::INT_16),
      make_parquet_type!("int32", PhysicalType::INT32, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("int64", PhysicalType::INT64, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("double", PhysicalType::DOUBLE, with_repetition: Repetition::OPTIONAL),
      make_parquet_type!("float", PhysicalType::FLOAT, with_repetition: Repetition::OPTIONAL),
      make_parquet_type!("string", PhysicalType::BYTE_ARRAY,
        with_repetition: Repetition::OPTIONAL, with_logical_type: LogicalType::UTF8),
    ];

    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();

    let parquet_schema = SchemaDescriptor::new(Rc::new(parquet_group_type));
    let converted_arrow_schema = parquet_to_arrow_schema(Rc::new(parquet_schema)).unwrap();

    let arrow_fields = vec![
      Field::new("boolean", DataType::Boolean, false),
      Field::new("int8", DataType::Int8, false),
      Field::new("int16", DataType::Int16, false),
      Field::new("int32", DataType::Int32, false),
      Field::new("int64", DataType::Int64, false),
      Field::new("double", DataType::Float64, true),
      Field::new("float", DataType::Float32, true),
      Field::new("string", DataType::Utf8, true),
    ];

    assert_eq!(&arrow_fields, converted_arrow_schema.fields());
  }

  #[test]
  fn test_duplicate_fields() {
    let mut parquet_types = vec![
      make_parquet_type!("boolean", PhysicalType::BOOLEAN, with_repetition: Repetition::REQUIRED),
      make_parquet_type!("int8", PhysicalType::INT32, with_repetition: Repetition::REQUIRED,
        with_logical_type: LogicalType::INT_8)
    ];

    let parquet_group_type = GroupTypeBuilder::new("")
      .with_fields(&mut parquet_types)
      .build()
      .unwrap();

    let parquet_schema = Rc::new(SchemaDescriptor::new(Rc::new(parquet_group_type)));
    let converted_arrow_schema = parquet_to_arrow_schema(parquet_schema.clone()).unwrap();

    let arrow_fields = vec![
      Field::new("boolean", DataType::Boolean, false),
      Field::new("int8", DataType::Int8, false),
    ];
    assert_eq!(&arrow_fields, converted_arrow_schema.fields());

    let converted_arrow_schema = parquet_to_arrow_schema_by_columns(
      parquet_schema.clone(), vec![0usize, 1usize])
      .unwrap();
    assert_eq!(&arrow_fields, converted_arrow_schema.fields());
  }

  #[test]
  fn test_parquet_lists() {

  }
}
