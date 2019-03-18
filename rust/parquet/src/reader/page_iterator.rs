use crate::column::page::PageReader;
use crate::file::reader::FileReader;
use crate::schema::types::SchemaDescPtr;
use crate::schema::types::ColumnDescPtr;

use crate::errors::Result;
use crate::errors::check_in_range;

pub trait PageIterator:  Iterator<Item=Result<Box<PageReader>>> {
    fn schema(&mut self) -> Result<SchemaDescPtr>;
    fn column_schema(&mut self) -> Result<ColumnDescPtr>;
}

struct FilePageIterator<T>
    where T: Iterator<Item=usize>
{
    column_index: usize,
    row_group_indices: T,
    file_reader: Box<FileReader>,
}

impl<T> FilePageIterator<T>
    where T: Iterator<Item=usize>
{
    pub fn new(column_index: usize, row_group_indices: T, file_reader: Box<FileReader>)
        -> Result<Self> {
        // Check that column_index are valid
        let num_columns = file_reader.metadata().file_metadata()
            .schema_descr_ptr().num_columns();

        check_in_range(column_index, num_columns)?;

        // We don't check iterators here because iterator may be infinite

        Ok(Self {
            column_index,
            row_group_indices,
            file_reader
        })
    }
}

impl<T> Iterator for FilePageIterator<T>
    where T: Iterator<Item=usize>
{
    type Item = Result<Box<PageReader>>;

    fn next(&mut self) -> Option<Result<Box<PageReader>>> {
        self.row_group_indices
            .next()
            .map(|row_group_index| { self.file_reader
                .get_row_group(row_group_index)
                .and_then(|r| r.get_column_page_reader(self.column_index))
        })
    }
}

impl<T> PageIterator for FilePageIterator<T>
    where T: Iterator<Item=usize>
{
    fn schema(&mut self) -> Result<SchemaDescPtr>  {
        Ok(self.file_reader.metadata()
            .file_metadata()
            .schema_descr_ptr())
    }

    fn column_schema(&mut self) -> Result<ColumnDescPtr> {
        self.schema()
            .map(|s| s.column(self.column_index))
    }
}
