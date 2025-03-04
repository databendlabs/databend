// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_ipc::reader::FileReaderBuilder;
use arrow_schema::Schema;
use buf_list::BufList;
use buf_list::Cursor;
use bytes::Buf;
use databend_common_base::base::Alignment;
use databend_common_base::base::DmaWriteBuf;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::write_column;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Value;
use opendal::Buffer;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;
use parquet::format::FileMetaData;

#[derive(Debug, Clone)]
pub enum Layout {
    ArrowIpc(Box<[usize]>),
    Parquet,
    Aggregate,
}

pub(super) struct BlocksEncoder {
    pub(super) use_parquet: bool,
    pub(super) buf: DmaWriteBuf,
    pub(super) offsets: Vec<usize>,
    pub(super) columns_layout: Vec<Layout>,
}

impl BlocksEncoder {
    pub(super) fn new(use_parquet: bool, align: Alignment, chunk: usize) -> Self {
        Self {
            use_parquet,
            buf: DmaWriteBuf::new(align, chunk),
            offsets: vec![0],
            columns_layout: Vec::new(),
        }
    }

    pub(super) fn add_blocks(&mut self, mut blocks: Vec<DataBlock>) {
        let layout = if self.use_parquet {
            // Currently we splice multiple complete parquet files into one,
            // so that the file contains duplicate headers/footers and metadata,
            // which can lead to file bloat. A better approach would be for the entire file to be ONE parquet,
            // with each group of blocks (i.e. Chunk) corresponding to one or more row groupsx
            bare_blocks_to_parquet(blocks, &mut self.buf).unwrap();
            Layout::Parquet
        } else {
            let block = if blocks.len() == 1 {
                blocks.remove(0)
            } else {
                DataBlock::concat(&blocks).unwrap()
            };
            let columns_layout = std::iter::once(self.size())
                .chain(block.columns().iter().map(|entry| {
                    let column = entry
                        .value
                        .convert_to_full_column(&entry.data_type, block.num_rows());
                    write_column(&column, &mut self.buf).unwrap();
                    self.size()
                }))
                .map_windows(|x: &[_; 2]| x[1] - x[0])
                .collect::<Vec<_>>()
                .into_boxed_slice();

            Layout::ArrowIpc(columns_layout)
        };

        self.columns_layout.push(layout);
        self.offsets.push(self.size())
    }

    pub(super) fn size(&self) -> usize {
        self.buf.size()
    }
}

pub(super) fn deserialize_block(columns_layout: &Layout, data: Buffer) -> Result<DataBlock> {
    match columns_layout {
        Layout::ArrowIpc(layout) => bare_blocks_from_arrow_ipc(layout, data),
        Layout::Parquet => bare_blocks_from_parquet(Reader(data)),
        Layout::Aggregate => unreachable!(),
    }
}

fn fake_data_schema(block: &DataBlock) -> DataSchema {
    let fields = block
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone()))
        .collect::<Vec<_>>();
    DataSchema::new(fields)
}

fn bare_blocks_from_arrow_ipc(layout: &[usize], mut data: Buffer) -> Result<DataBlock> {
    assert!(!layout.is_empty());
    let mut columns = Vec::with_capacity(layout.len());
    let mut read_array = |layout: usize| -> Result<(ArrayRef, DataType)> {
        let ls = BufList::from_iter(data.slice(0..layout));
        data.advance(layout);
        let mut reader = FileReaderBuilder::new().build(Cursor::new(ls))?;
        let schema = reader.schema();
        let f = DataField::try_from(schema.field(0))?;
        let data_type = f.data_type().clone();
        let col = reader
            .next()
            .ok_or_else(|| ErrorCode::Internal("expected one arrow array"))??
            .remove_column(0);
        Ok((col, data_type))
    };
    let (array, data_type) = read_array(layout[0])?;
    let num_rows = array.len();
    let val = Value::from_arrow_rs(array, &data_type)?;
    columns.push(BlockEntry::new(data_type, val));
    for &layout in layout.iter().skip(1) {
        let (array, data_type) = read_array(layout)?;
        let val = Value::from_arrow_rs(array, &data_type)?;
        columns.push(BlockEntry::new(data_type, val));
    }
    Ok(DataBlock::new(columns, num_rows))
}

/// Deserialize bare data block from parquet format.
fn bare_blocks_from_parquet<R: ChunkReader + 'static>(data: R) -> Result<DataBlock> {
    let reader = ParquetRecordBatchReader::try_new(data, usize::MAX)?;
    let mut blocks = Vec::new();
    for record_batch in reader {
        let record_batch = record_batch?;
        let schema = DataSchema::try_from(record_batch.schema().as_ref())?;
        let num_rows = record_batch.num_rows();
        let mut columns = Vec::with_capacity(record_batch.num_columns());
        for (array, field) in record_batch.columns().iter().zip(schema.fields()) {
            let data_type = field.data_type();
            columns.push(BlockEntry::new(
                data_type.clone(),
                Value::from_arrow_rs(array.clone(), data_type)?,
            ))
        }
        let block = DataBlock::new(columns, num_rows);
        blocks.push(block);
    }

    if blocks.len() == 1 {
        Ok(blocks.remove(0))
    } else {
        DataBlock::concat(&blocks)
    }
}

/// Serialize bare data blocks to parquet format.
fn bare_blocks_to_parquet<W: Write + Send>(
    blocks: Vec<DataBlock>,
    write_buffer: W,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());

    let data_schema = fake_data_schema(blocks.first().unwrap());
    let table_schema = infer_table_schema(&data_schema)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .build();
    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch(&table_schema))
        .collect::<Result<Vec<_>>>()?;
    let arrow_schema = Arc::new(Schema::from(table_schema.as_ref()));
    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

pub struct Reader(pub Buffer);

impl Length for Reader {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
}

impl ChunkReader for Reader {
    type T = bytes::buf::Reader<Buffer>;

    fn get_read(&self, start: u64) -> errors::Result<Self::T> {
        let start = start as usize;
        if start > self.0.remaining() {
            return Err(errors::ParquetError::IndexOutOfBound(
                start,
                self.0.remaining(),
            ));
        }
        let mut r = self.0.clone();
        r.advance(start);
        Ok(r.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> errors::Result<bytes::Bytes> {
        let start = start as usize;
        Ok(self.0.slice(start..start + length).to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use databend_common_expression::block_debug::assert_block_value_eq;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::FromData;

    use super::*;

    #[test]
    fn test_serde_bin_column() -> Result<()> {
        let blocks = vec![
            [
                StringType::from_data(vec!["SM CASE", "a"]),
                StringType::from_data(vec!["SM CASE", "axx"]),
                Int64Type::from_data(vec![1, 3]),
            ],
            [
                StringType::from_data(vec!["b", "e", "f", "g"]),
                StringType::from_data(vec!["", "", "", "x"]),
                Int64Type::from_data(vec![99, 7, 3, 4]),
            ],
        ]
        .into_iter()
        .map(|columns| DataBlock::new_from_columns(columns.to_vec()))
        .collect::<Vec<_>>();

        let mut data = Vec::new();
        bare_blocks_to_parquet(blocks.clone(), &mut data)?;

        let reader = Reader(Buffer::from(Bytes::from(data)));

        let got = bare_blocks_from_parquet(reader)?;
        let want = DataBlock::concat(&blocks)?;

        assert_block_value_eq(&want, &got);

        Ok(())
    }
}
