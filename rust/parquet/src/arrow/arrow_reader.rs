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
        let mut array_reader = build_array_reader(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            column_indices,
            self.file_reader.clone(),
        )?;

//        dbg!(array_reader.get_data_type());
        array_reader.next_batch(record_num as usize)
    }
}

impl ParquetArrowReader {
    pub fn new(file_reader: Rc<FileReader>) -> Self {
        Self { file_reader }
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::arrow_reader::{ArrowReader, ParquetArrowReader};
    use crate::file::reader::{FileReader, SerializedFileReader};
    use arrow::array::{Array, ArrayEqual, ListArray, StructArray, PrimitiveArray};
    use arrow::datatypes::{Int32Type, Int64Type};
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;
    use std::rc::Rc;

    #[test]
    fn test_arrow_read_schema() {
        let mut arrow_reader =
            ParquetArrowReader::new(get_parquet_test_data("blogs/blogs.parquet"));

        //        let schema = arrow_reader.get_schema().expect("Failed to read schema!");
        //        println!("Schema is : {}", schema.to_json());

        let reader = arrow_reader
            .read_into_array(0..10)
            .expect("Failed to read into array!");

        let blog_array = reader
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Blog array should be a struct array");
        assert_eq!(5, blog_array.len());
        assert_eq!(3, blog_array.num_columns());

        let user_array = blog_array
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("User array should be a struct array");

        assert_eq!(5, user_array.len());
        assert_eq!(2, user_array.num_columns());

        let id_array = user_array
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .expect("Id array should be a int32 array!");
        let expected_id_array = PrimitiveArray::<Int32Type>::from(vec![
            Some(-1470019348),
            Some(285238895),
            Some(1126016969),
            Some(962878313),
            Some(1925670629),
        ]);
        assert!(id_array.equals(&expected_id_array));

        let name_id_array = user_array
            .column(1)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .expect("Name id array should be a int32 array!");
        let expected_name_id_array = PrimitiveArray::<Int64Type>::from(vec![
            None,
            Some(-3840759436675050715i64),
            Some(-60094454035794972),
            Some(-3652070941637861073),
            None,
        ]);
        assert!(name_id_array.equals(&expected_name_id_array));

        let body_id_array = blog_array
            .column(1)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .expect("Body id array should be a struct array");
        let expected_body_id_array = PrimitiveArray::<Int64Type>::from(vec![
            Some(-5895729336325872978i64),
            Some(-1129780966275687003i64),
            Some(5337695475218340151i64),
            Some(-5567457004549944277i64),
            Some(7941311309475484289i64),
        ]);
        assert!(body_id_array.equals(&expected_body_id_array));

        let comment_list_array = blog_array
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Comment array should be a list array");

        assert_eq!(5, comment_list_array.len());

        let comment_struct_array = comment_list_array.values();
        let comment_struct_array = comment_struct_array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Comment struct array should be a struct array");

        assert_eq!(14, comment_struct_array.len());
        assert_eq!(1, comment_struct_array.num_columns());

        let comment_user_id_array = comment_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int32Type>>()
            .expect("Comment user id array should be a primitive array");

        let expected_comment_user_id_array = PrimitiveArray::<Int32Type>::from(vec![
            Some(1416669008),
            Some(-1010674453),
            Some(1017918520),
            Some(-1751665155),
            Some(-1504515773),
            Some(529998189),
            Some(351252315),
            Some(-933631421),
            Some(473563087),
            Some(1691915172),
            Some(-207011839),
            Some(-675603674),
            Some(1159397102),
            Some(-1268818396),
        ]);

        assert!(dbg!(comment_user_id_array).equals(dbg!(&expected_comment_user_id_array)));
    }

    fn get_parquet_test_data(file_name: &str) -> Rc<FileReader> {
        let mut path = PathBuf::new();
        path.push(env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined!"));
        path.push(file_name);

        let file = File::open(path.as_path()).expect("File not found!");

        let reader =
            SerializedFileReader::new(file).expect("Failed to create serialized reader");

        Rc::new(reader)
    }
}
