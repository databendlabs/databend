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

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::StructArray;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::TableSchema;

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

#[derive(Default)]
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
    pub summary: Statistics,
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
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Arc<dyn AbstractSegment>> {
        let this = std::mem::take(self);
        let columns = vec![
            Arc::new(UInt64Array::from(this.row_count)) as ArrayRef,
            Arc::new(UInt64Array::from(this.block_size)) as ArrayRef,
            Arc::new(UInt64Array::from(this.file_size)) as ArrayRef,
            Arc::new(StructArray::try_new(
                location_fields(),
                vec![
                    Arc::new(StringArray::from(this.location_path)) as ArrayRef,
                    Arc::new(UInt64Array::from(this.location_version)) as ArrayRef,
                ],
                None,
            )?),
            Arc::new(UInt64Array::from(this.bloom_filter_index_size)) as ArrayRef,
            Arc::new(UInt64Array::from(this.inverted_index_size)) as ArrayRef,
            Arc::new(UInt8Array::from(this.compression)) as ArrayRef,
        ];
        let block_metas = RecordBatch::try_new(segment_schema(), columns)?;
        Ok(Arc::new(ColumnOrientedSegment {
            block_metas,
            summary: this.summary,
        }))
    }
}
