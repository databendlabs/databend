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

use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::StructArray;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::SchemaRef;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::TableSchemaRef;

use super::schema::location_fields;
use super::schema::segment_schema;
use super::segment::ColumnOrientedSegment;
use crate::meta::AbstractSegment;
use crate::meta::BlockMeta;
use crate::meta::Statistics;

pub trait SegmentBuilder: Send + Sync + 'static {
    fn block_count(&self) -> usize;
    fn add_block(&mut self, block_meta: BlockMeta);
    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Arc<dyn AbstractSegment>>;
}

pub struct ColumnOrientedSegmentBuilder {
    pub row_count: Vec<u64>,
    pub block_size: Vec<u64>,
    pub file_size: Vec<u64>,
    pub location_path: Vec<String>,
    pub location_version: Vec<u64>,
    pub bloom_filter_index_location_path: Vec<Option<String>>,
    pub bloom_filter_index_location_version: Vec<Option<u64>>,
    pub bloom_filter_index_size: Vec<u64>,
    pub inverted_index_size: Vec<Option<u64>>,
    pub compression: Vec<u8>,
    pub create_on: Vec<Option<i64>>,
    pub summary: Statistics,
    pub segment_schema: SchemaRef,
}

impl ColumnOrientedSegmentBuilder {
    pub fn new(table_schema: TableSchemaRef) -> Self {
        Self {
            row_count: vec![],
            block_size: vec![],
            file_size: vec![],
            location_path: vec![],
            location_version: vec![],
            bloom_filter_index_location_path: vec![],
            bloom_filter_index_location_version: vec![],
            bloom_filter_index_size: vec![],
            inverted_index_size: vec![],
            compression: vec![],
            create_on: vec![],
            summary: Statistics::default(),
            segment_schema: segment_schema(table_schema),
        }
    }
}

impl SegmentBuilder for ColumnOrientedSegmentBuilder {
    fn block_count(&self) -> usize {
        self.row_count.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) {
        self.row_count.push(block_meta.row_count);
        self.block_size.push(block_meta.block_size);
        self.file_size.push(block_meta.file_size);
        self.location_path.push(block_meta.location.0);
        self.location_version.push(block_meta.location.1);
        self.bloom_filter_index_size
            .push(block_meta.bloom_filter_index_size);
        self.inverted_index_size
            .push(block_meta.inverted_index_size);
        self.compression.push(block_meta.compression.to_u8());
        self.create_on
            .push(block_meta.create_on.map(|t| t.timestamp()));
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Arc<dyn AbstractSegment>> {
        let columns = vec![
            Arc::new(UInt64Array::from(std::mem::take(&mut self.row_count))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(&mut self.block_size))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(&mut self.file_size))) as ArrayRef,
            Arc::new(StructArray::try_new(
                location_fields(),
                vec![
                    Arc::new(StringArray::from(std::mem::take(&mut self.location_path)))
                        as ArrayRef,
                    Arc::new(UInt64Array::from(std::mem::take(
                        &mut self.location_version,
                    ))) as ArrayRef,
                ],
                None,
            )?),
            Arc::new(UInt64Array::from(std::mem::take(
                &mut self.bloom_filter_index_size,
            ))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(
                &mut self.inverted_index_size,
            ))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.compression))) as ArrayRef,
        ];
        let block_metas = RecordBatch::try_new(self.segment_schema.clone(), columns)?;
        Ok(Arc::new(ColumnOrientedSegment {
            block_metas,
            summary: std::mem::take(&mut self.summary),
        }))
    }
}
