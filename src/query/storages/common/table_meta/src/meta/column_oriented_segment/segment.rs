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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::Schema;
use bytes::Bytes;
use databend_common_column::binview::BinaryViewColumnGeneric;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::types::Buffer;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::properties::WriterProperties;

use super::BLOCK_SIZE;
use super::BLOOM_FILTER_INDEX_SIZE;
use super::COMPRESSION;
use super::FILE_SIZE;
use super::LOCATION_PATH;
use super::ROW_COUNT;
use super::block_meta::AbstractBlockMeta;
use super::block_meta::ColumnOrientedBlockMeta;
use super::meta_name;
use super::stat_name;
use crate::meta::BlockMeta;
use crate::meta::CompactSegmentInfo;
use crate::meta::MetaCompression;
use crate::meta::MetaEncoding;
use crate::meta::SegmentInfo;
use crate::meta::Statistics;
use crate::meta::column_oriented_segment::LOCATION;
use crate::meta::format::compress;
use crate::meta::format::decode;
use crate::meta::format::decompress;
use crate::meta::format::encode;

pub trait AbstractSegment: Send + Sync + 'static + Sized {
    type BlockMeta: AbstractBlockMeta;
    fn block_metas(&self) -> Result<Vec<Arc<Self::BlockMeta>>>;
    fn summary(&self) -> &Statistics;
    fn serialize(&self) -> Result<Vec<u8>>;
    fn concat(v: Vec<Self>, summary: Statistics) -> Result<Self>;
}

impl AbstractSegment for SegmentInfo {
    type BlockMeta = BlockMeta;
    fn block_metas(&self) -> Result<Vec<Arc<Self::BlockMeta>>> {
        let blocks = self.blocks.to_vec();
        Ok(blocks)
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        self.to_bytes()
    }

    fn summary(&self) -> &Statistics {
        &self.summary
    }

    fn concat(mut v: Vec<Self>, summary: Statistics) -> Result<Self> {
        let mut blocks = Vec::with_capacity(v.len());
        for segment in v.iter_mut() {
            blocks.append(&mut segment.blocks);
        }
        Ok(Self::new(blocks, summary))
    }
}

impl AbstractSegment for CompactSegmentInfo {
    type BlockMeta = BlockMeta;
    fn block_metas(&self) -> Result<Vec<Arc<Self::BlockMeta>>> {
        self.block_metas()
    }

    fn summary(&self) -> &Statistics {
        &self.summary
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn concat(_v: Vec<Self>, _summary: Statistics) -> Result<Self> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct ColumnOrientedSegment {
    pub block_metas: DataBlock,
    pub summary: Statistics,
    pub segment_schema: TableSchema,
}

impl ColumnOrientedSegment {
    pub fn contains_col(&self, col_name: &str) -> bool {
        self.segment_schema.column_with_name(col_name).is_some()
    }

    pub fn stat_col(&self, col_id: u32) -> Option<Column> {
        let stat_name = stat_name(col_id);
        self.col_by_name(&[&stat_name])
    }

    pub fn meta_col(&self, col_id: u32) -> Option<Column> {
        let meta_name = meta_name(col_id);
        self.col_by_name(&[&meta_name])
    }

    pub fn row_count_col(&self) -> Buffer<u64> {
        self.col_by_name(&[ROW_COUNT])
            .unwrap()
            .as_number()
            .unwrap()
            .as_u_int64()
            .unwrap()
            .clone()
    }

    pub fn block_size_col(&self) -> Buffer<u64> {
        self.col_by_name(&[BLOCK_SIZE])
            .unwrap()
            .as_number()
            .unwrap()
            .as_u_int64()
            .unwrap()
            .clone()
    }

    pub fn file_size_col(&self) -> Buffer<u64> {
        self.col_by_name(&[FILE_SIZE])
            .unwrap()
            .as_number()
            .unwrap()
            .as_u_int64()
            .unwrap()
            .clone()
    }

    pub fn location_path_col(&self) -> BinaryViewColumnGeneric<str> {
        self.col_by_name(&[LOCATION, LOCATION_PATH])
            .unwrap()
            .as_string()
            .unwrap()
            .clone()
    }

    pub fn compression_col(&self) -> Buffer<u8> {
        self.col_by_name(&[COMPRESSION])
            .unwrap()
            .as_number()
            .unwrap()
            .as_u_int8()
            .unwrap()
            .clone()
    }

    pub fn col_meta_cols(&self, col_ids: &HashSet<ColumnId>) -> HashMap<ColumnId, Column> {
        let mut col_metas = HashMap::new();
        for col_id in col_ids {
            let meta_name = meta_name(*col_id);
            let meta_col = self.col_by_name(&[&meta_name]);
            if let Some(meta_col) = meta_col {
                col_metas.insert(*col_id, meta_col);
            }
        }
        col_metas
    }

    pub fn bloom_filter_index_size_col(&self) -> Buffer<u64> {
        self.col_by_name(&[BLOOM_FILTER_INDEX_SIZE])
            .unwrap()
            .as_number()
            .unwrap()
            .as_u_int64()
            .unwrap()
            .clone()
    }

    pub fn col_by_name(&self, name: &[&str]) -> Option<Column> {
        let (index, field) = self.segment_schema.column_with_name(name[0])?;
        let column = self.block_metas.get_by_offset(index).to_column();
        if name.len() == 1 {
            Some(column)
        } else {
            let sub_cols = column.as_tuple().unwrap();
            match &field.data_type {
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                } => Self::col_by_name_inner(&name[1..], sub_cols, fields_name, fields_type),
                _ => panic!("expect tuple type"),
            }
        }
    }

    fn col_by_name_inner(
        name: &[&str],
        cols: &[Column],
        field_names: &[String],
        field_types: &[TableDataType],
    ) -> Option<Column> {
        let index = field_names.iter().position(|f| f == name[0])?;
        let column = cols[index].clone();
        if name.len() == 1 {
            Some(column)
        } else {
            let sub_cols = column.as_tuple().unwrap();
            match &field_types[index] {
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                } => Self::col_by_name_inner(&name[1..], sub_cols, fields_name, fields_type),
                _ => panic!("expect tuple type"),
            }
        }
    }

    pub fn block_metas(&self) -> DataBlock {
        self.block_metas.clone()
    }
}

impl AbstractSegment for ColumnOrientedSegment {
    type BlockMeta = ColumnOrientedBlockMeta;
    fn block_metas(&self) -> Result<Vec<Arc<Self::BlockMeta>>> {
        let block_num = self.block_metas.num_rows();
        let mut block_metas = Vec::with_capacity(block_num);
        for i in 0..block_num {
            let block_meta = ColumnOrientedBlockMeta {
                segment: self.clone(),
                row_number: i,
            };
            block_metas.push(Arc::new(block_meta));
        }
        Ok(block_metas)
    }

    fn summary(&self) -> &Statistics {
        &self.summary
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        // TODO(Sky): Reuse the buffer.
        let mut write_buffer = Vec::new();
        let encoding = MetaEncoding::MessagePack;
        let compression = MetaCompression::default();
        {
            // TODO(Sky): Construct the optimal props, enabling compression, encoding, etc., if performance is better.
            let props = Some(
                WriterProperties::builder()
                    .set_max_row_group_size(usize::MAX)
                    .build(),
            );
            let arrow_schema = Arc::new(Schema::from(&self.segment_schema));
            let mut writer = ArrowWriter::try_new(&mut write_buffer, arrow_schema, props)?;
            writer.write(
                &self
                    .block_metas
                    .clone()
                    .to_record_batch(&self.segment_schema)?,
            )?;
            let _ = writer.close()?;
        }
        let blocks_size = write_buffer.len() as u64;
        {
            let summary = encode(&encoding, &self.summary)?;
            let summary_compress = compress(&compression, summary)?;
            // TODO(Sky): Avoid extra copy.
            write_buffer.extend(summary_compress);
        }
        let summary_size = write_buffer.len() as u64 - blocks_size;
        write_buffer.push(encoding as u8);
        write_buffer.push(compression as u8);
        write_buffer.extend_from_slice(&blocks_size.to_le_bytes());
        write_buffer.extend_from_slice(&summary_size.to_le_bytes());
        Ok(write_buffer)
    }

    fn concat(v: Vec<Self>, summary: Statistics) -> Result<Self> {
        let segment_schema = v[0].segment_schema.clone();
        let blocks = v.into_iter().map(|s| s.block_metas).collect::<Vec<_>>();
        let block_metas = DataBlock::concat(&blocks)?;
        Ok(Self {
            block_metas,
            summary,
            segment_schema,
        })
    }
}

/// Deserialize a column-oriented segment from bytes.
///
/// # Arguments
///
/// * `data`: The bytes of the segment.
/// * `projection`: The names of the columns to be deserialized.
/// * `need_summary`: Whether to need deserialize the summary of the segment.
///
/// # Returns
///
/// A tuple containing the deserialized data block, the schema of the segment, and the summary of the segment.
pub fn deserialize_column_oriented_segment(
    data: Bytes,
    projection: &HashSet<String>,
    need_summary: bool,
) -> Result<(DataBlock, TableSchema, Option<Statistics>)> {
    const FOOTER_SIZE: usize = 18;

    // 1. parse footer
    let footer = &data[data.len() - FOOTER_SIZE..];
    let encoding = MetaEncoding::try_from(footer[0])?;
    let compression = MetaCompression::try_from(footer[1])?;
    let blocks_size = u64::from_le_bytes(footer[2..10].try_into().unwrap()) as usize;
    let summary_size = u64::from_le_bytes(footer[10..].try_into().unwrap());

    // 2. deserialize block_metas
    let block_metas = data.slice(0..blocks_size);
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&block_metas)?;
    let schema = metadata.file_metadata().schema_descr_ptr();
    let mut mask = Vec::new();
    for (index, field) in schema.root_schema().get_fields().iter().enumerate() {
        if projection.contains(field.name()) {
            mask.push(index);
        }
    }
    let projection_mask = ProjectionMask::roots(&schema, mask);
    let mut record_reader = ParquetRecordBatchReaderBuilder::try_new(block_metas)?
        .with_batch_size(usize::MAX)
        .with_projection(projection_mask)
        .build()?;
    let batch = record_reader.next().unwrap()?;
    let data_schema = DataSchema::try_from(&(*batch.schema()))?;
    let block_metas = DataBlock::from_record_batch(&data_schema, &batch)?;
    assert!(record_reader.next().is_none());

    // 3. deserialize summary
    let summary = if !need_summary {
        None
    } else {
        // TODO(Sky): Avoid extra copy.
        let summary = data[blocks_size..blocks_size + summary_size as usize].to_vec();
        let summary = decompress(&compression, summary)?;
        let summary = decode(&encoding, &summary)?;
        Some(summary)
    };
    Ok((
        block_metas,
        TableSchema::try_from(&(*batch.schema()))?,
        summary,
    ))
}
