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

use std::sync::Arc;

use arrow::datatypes::Schema;
use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::format::compress;
use databend_storages_common_table_meta::meta::format::decode;
use databend_storages_common_table_meta::meta::format::decompress;
use databend_storages_common_table_meta::meta::format::encode;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::MetaCompression;
use databend_storages_common_table_meta::meta::MetaEncoding;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::meta_name;
use super::stat_name;
use crate::io::read::meta::bytes_reader;
use crate::io::SegmentsIO;

#[async_trait::async_trait]
pub trait AbstractSegment: Send + Sync + 'static + Sized {
    fn summary(&self) -> &Statistics;
    fn serialize(&self) -> Result<Vec<u8>>;
    async fn read_and_deserialize(
        dal: Operator,
        location: Location,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<Self>>;
}

#[async_trait::async_trait]
impl AbstractSegment for SegmentInfo {
    fn serialize(&self) -> Result<Vec<u8>> {
        self.to_bytes()
    }

    fn summary(&self) -> &Statistics {
        &self.summary
    }

    async fn read_and_deserialize(
        _dal: Operator,
        _location: Location,
        _table_schema: TableSchemaRef,
        _put_cache: bool,
    ) -> Result<Arc<Self>> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl AbstractSegment for CompactSegmentInfo {
    fn summary(&self) -> &Statistics {
        &self.summary
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        unimplemented!()
    }

    async fn read_and_deserialize(
        dal: Operator,
        location: Location,
        table_schema: TableSchemaRef,
        put_cache: bool,
    ) -> Result<Arc<Self>> {
        SegmentsIO::read_compact_segment(dal, location, table_schema, put_cache).await
    }
}

pub struct ColumnOrientedSegment {
    pub block_metas: DataBlock,
    pub summary: Statistics,
    pub segment_schema: TableSchema,
}

impl ColumnOrientedSegment {
    pub fn stat_col(&self, col_id: u32) -> Option<Column> {
        let stat_name = stat_name(col_id);
        self.col_by_name(&[&stat_name])
    }

    pub fn meta_col(&self, col_id: u32) -> Option<Column> {
        let meta_name = meta_name(col_id);
        self.col_by_name(&[&meta_name])
    }

    pub fn col_by_name(&self, name: &[&str]) -> Option<Column> {
        let (index, field) = self.segment_schema.column_with_name(name[0])?;
        let column = self
            .block_metas
            .get_by_offset(index)
            .to_column(self.block_metas.num_rows());
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
}

#[async_trait::async_trait]
impl AbstractSegment for ColumnOrientedSegment {
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

    // TODO(Sky): populate cache
    async fn read_and_deserialize(
        dal: Operator,
        location: Location,
        _table_schema: TableSchemaRef,
        _put_cache: bool,
    ) -> Result<Arc<Self>> {
        let reader = bytes_reader(&dal, &location.0, None).await?;
        deserialize_column_oriented_segment(reader.to_bytes())
    }
}

fn deserialize_column_oriented_segment(data: Bytes) -> Result<Arc<ColumnOrientedSegment>> {
    const FOOTER_SIZE: usize = 18;
    let footer = &data[data.len() - FOOTER_SIZE..];
    let encoding = MetaEncoding::try_from(footer[0])?;
    let compression = MetaCompression::try_from(footer[1])?;
    let blocks_size = u64::from_le_bytes(footer[2..10].try_into().unwrap()) as usize;
    let summary_size = u64::from_le_bytes(footer[10..].try_into().unwrap());

    let block_metas = data.slice(0..blocks_size);
    let mut record_reader = ParquetRecordBatchReader::try_new(block_metas, usize::MAX)?;
    let batch = record_reader.next().unwrap()?;
    let data_schema = DataSchema::try_from(&(*batch.schema()))?;
    let (block_metas, _) = DataBlock::from_record_batch(&data_schema, &batch)?;
    assert!(record_reader.next().is_none());

    // TODO(Sky): Avoid extra copy.
    let summary = data[blocks_size..blocks_size + summary_size as usize].to_vec();
    let summary = decompress(&compression, summary)?;
    let summary = decode(&encoding, &summary)?;
    Ok(Arc::new(ColumnOrientedSegment {
        block_metas,
        summary,
        segment_schema: TableSchema::try_from(&(*batch.schema()))?,
    }))
}
