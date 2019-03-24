use std::cmp::{min, max};
use std::mem::transmute;
use std::mem::size_of;
use std::mem::replace;
use std::slice;

use arrow::buffer::MutableBuffer;
use arrow::builder::BooleanBufferBuilder;
use crate::data_type::DataType;
use crate::errors::Result;
use crate::errors::ParquetError;
use crate::schema::types::ColumnDescPtr;
use crate::column::page::PageReader;
use crate::column::reader::ColumnReaderImpl;
use arrow::buffer::Buffer;
use arrow::builder::BufferBuilderTrait;

const MIN_BATCH_SIZE: usize = 1024;

pub struct RecordReader<T: DataType> {
  records: MutableBuffer,
  def_levels: Option<MutableBuffer>,
  rep_levels: Option<MutableBuffer>,
  bitmaps: Option<BooleanBufferBuilder>,
  column_reader: ColumnReaderImpl<T>,
  column_schema: ColumnDescPtr,

  /// Number of records seen
  records_num: usize,
  /// Starts from 1, ```values_pos-1``` is end position of #```records_num``` records
  values_pos: usize,
  /// Starts from 1, number of values have been written to buffer
  values_written: usize,
}

impl<T: DataType> RecordReader<T> {
  pub fn new(column_schema: ColumnDescPtr, page_reader: Box<PageReader>) -> Self {
    let def_levels = if column_schema.max_rep_level()>0 {
      Some(MutableBuffer::new(MIN_BATCH_SIZE))
    } else {
      None
    };

    let rep_levels = if column_schema.max_def_level()>0 {
      Some(MutableBuffer::new(MIN_BATCH_SIZE))
    } else {
      None
    };

    let bitmaps = if column_schema.max_def_level()>0 {
      Some(BooleanBufferBuilder::new(MIN_BATCH_SIZE))
    } else {
      None
    };

    Self {
      records: MutableBuffer::new(MIN_BATCH_SIZE),
      def_levels,
      rep_levels,
      bitmaps,
      column_reader: ColumnReaderImpl::new(column_schema.clone(), page_reader),
      column_schema,
      records_num: 0usize,
      values_pos: 0usize,
      values_written: 0usize
    }
  }

  pub fn read_records(&mut self, num_records: usize) -> Result<usize> {
    let mut records_read = 0usize;
    let mut end_of_column = false;

    loop {
      let (iter_record_num, last_record_value_pos) =
        self.split_and_count_records(num_records-records_read, end_of_column)?;

      records_read += iter_record_num;
      self.values_pos = last_record_value_pos;
      self.records_num += iter_record_num;

      if records_read >= num_records {
        return Ok(records_read)
      }

      if end_of_column {
        break;
      }

      let batch_size =  max(num_records - records_read, MIN_BATCH_SIZE);

      let values_read = self.read_one_batch(batch_size)?;
      if values_read==0 {
        end_of_column = true;
      }
    }

    Ok(records_read)
  }

  pub fn records_num(&self) -> usize {
    self.records_num
  }

  pub fn records_data(&self) -> Result<&[u8]> {
    Ok(self.records.data())
  }

  pub fn repetition_levels_data(&self) -> Option<&[i16]> {
    unsafe {
      self.rep_levels.as_ref().map(|buf| {
        slice::from_raw_parts(
          transmute::<*const u8, *const i16>(buf.raw_data()),
          buf.capacity() / size_of::<i16>())
      })
    }
  }

  pub fn definition_levels_data(&self) -> Option<&[i16]> {
    unsafe {
      self.def_levels.as_ref().map(|buf| {
        slice::from_raw_parts(
          transmute::<*const u8, *const i16>(buf.raw_data()),
          buf.capacity() / size_of::<i16>())
      })
    }
  }

  pub fn reset(&mut self) -> Result<()> {
    self.records_num = 0;
    self.values_pos = 0;
    self.set_values_written(0)
  }

  pub fn set_page_reader(&mut self, page_reader: Box<PageReader>) -> Result<()> {
    self.column_reader = ColumnReaderImpl::new(self.column_schema.clone(), page_reader);
    Ok(())
  }

  pub fn consume_data(&mut self) -> Buffer {
    replace(&mut self.records, MutableBuffer::new(MIN_BATCH_SIZE))
        .freeze()
  }

  pub fn consume_null_bit_buffer(&mut self) -> Option<Buffer> {
    let bitmap_builder = if self.column_schema.max_def_level()>0 {
      Some(BooleanBufferBuilder::new(MIN_BATCH_SIZE))
    } else {
      None
    };

    replace(&mut self.bitmaps, bitmap_builder)
        .map(|mut builder| builder.finish())
  }

  pub fn values_num(&mut self) -> usize {
    self.values_written
  }


  fn read_one_batch(&mut self, batch_size: usize) -> Result<usize> {
    // Reserve spaces
    self.records.reserve(self.records.len()+batch_size*T::get_type_size())?;
    self.rep_levels.iter_mut().try_for_each(|buf| {
      buf.reserve(buf.len()+batch_size*size_of::<i16>())
        .map(|_| ())
    })?;
    self.def_levels.iter_mut().try_for_each(|buf| {
      buf.reserve(buf.len()+batch_size*size_of::<i16>())
        .map(|_| ())
    })?;


//    let data_buf = self.record_data_buf(self.values_written);
    let data_buf = unsafe {
      slice::from_raw_parts_mut(
        transmute::<*const u8, *mut T::T>(self.records.raw_data()).add(self.values_written),
        self.records.capacity() / T::get_type_size()-self.values_written
      )
    };
//    let def_levels_buf = self.def_levels_buf(self.values_written);
    let def_levels_buf = unsafe {
      self.def_levels.as_ref().map(|buf| { slice::from_raw_parts_mut(
        transmute::<*const u8, *mut i16>(buf.raw_data()).add(self.values_written),
        buf.capacity() / size_of::<i16>()-self.values_written)
      })
    };
//    let rep_levels_buf = self.rep_levels_buf(self.values_written);
    let rep_levels_buf = unsafe {
      self.rep_levels.as_ref().map(|buf| { slice::from_raw_parts_mut(
        transmute::<*const u8, *mut i16>(buf.raw_data()).add(self.values_written),
        buf.capacity() / size_of::<i16>()-self.values_written)
      })
    };
    let (data_read, levels_read) = self.column_reader.read_batch(
      batch_size, def_levels_buf, rep_levels_buf, data_buf)?;

    if data_read < levels_read {
      // Fill in spaces
      //TODO: Move this into ColumnReader
      let def_levels_buf = unsafe {
        self.def_levels.as_ref().map(|buf| { slice::from_raw_parts_mut(
          transmute::<*const u8, *mut i16>(buf.raw_data()).add(self.values_written),
          buf.capacity() / size_of::<i16>()-self.values_written)
        })
      };

      let def_levels_buf = def_levels_buf.ok_or_else(|| {
        general_err!("Definition levels should exist when data is less than levels!")
      })?;
      let mut data_pos = data_read-1;
      let mut level_pos = levels_read-1;

      while level_pos >= self.values_written {
        if def_levels_buf[level_pos] != 0 {
          if level_pos != data_pos {
            data_buf.swap(level_pos, data_pos);
          }
          data_pos -= 1;
        }

        level_pos -= 1;
      }
    }

    // Fill in bitmap data
//    if let Some(&mut notnull_buffer) = self.bitmaps {
//      (0..levels_read).for_each(|idx| {
//        notnull_buffer.append(def_levels_buf[idx] == self.column_schema.max_def_level())
//      })
//    }

    let values_read = max(data_read, levels_read);
    self.set_values_written(self.values_written+values_read)?;
    Ok(values_read)
  }

  fn split_and_count_records(&mut self, records_to_read: usize, end_of_column: bool)
    -> Result<(usize, usize)> {
    match  self.rep_levels_buf(0) {
      Some(buf) => {
        let mut records_read = 0usize;
        let mut last_value_pos = self.values_pos;

        let mut cur_value_pos = self.values_pos-1;

        while (cur_value_pos < self.values_written) && (records_read < records_to_read) {
          if buf[cur_value_pos] == 0 {
            records_read += 1;
            last_value_pos = cur_value_pos;
          }
          cur_value_pos += 1;
        }

        if (records_read<records_to_read) && (last_value_pos < self.values_written) &&
          end_of_column {
          last_value_pos = self.values_written;
          records_read += 1;
        }

        Ok((records_read, last_value_pos))
      }
      None => {
        let records_to_read = min(records_to_read, self.values_written-self.values_pos);
        Ok((records_to_read, self.values_pos + records_to_read))
      }
    }
  }

  #[inline]
  fn rep_levels_buf(&self, start: usize) -> Option<&mut [i16]> {
    unsafe {
      self.rep_levels.as_ref().map(|buf| {
        slice::from_raw_parts_mut(
          transmute::<*const u8, *mut i16>(buf.raw_data()).add(start),
          buf.capacity() / size_of::<i16>()-start)
      })
    }
  }

  #[inline]
  fn def_levels_buf(&self, start: usize) -> Option<&mut [i16]> {
    unsafe {
      self.def_levels.as_ref().map(|buf| {
        slice::from_raw_parts_mut(
          transmute::<*const u8, *mut i16>(buf.raw_data()).add(start),
          buf.capacity() / size_of::<i16>()-start)
      })
    }
  }

//  #[inline]
//  fn record_data_buf(&self, start: usize) -> &mut [T::T] {
//    unsafe {
//      slice::from_raw_parts_mut(
//        transmute::<*const u8, *mut T::T>(self.records.raw_data()).add(start),
//        self.records.capacity() / T::get_type_size()-start
//      )
//    }
//  }

  fn set_values_written(&mut self, new_values_written: usize) -> Result<()> {
    self.values_written = new_values_written;
    self.records.resize(self.records.len()+self.values_written*T::get_type_size())?;

    let new_levels_len = self.values_written*size_of::<i16>();
    self.rep_levels.iter_mut().try_for_each(|buf| {
      buf.resize(new_levels_len).map(|_| ())
    })?;
    self.def_levels.iter_mut().try_for_each(|buf| {
      buf.resize(new_levels_len).map(|_| ())
    })?;
    Ok(())
  }
}

mod tests {

}
