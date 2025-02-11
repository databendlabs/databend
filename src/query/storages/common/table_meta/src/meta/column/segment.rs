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

use arrow::array::RecordBatch;
use databend_common_exception::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::block_meta::ColumnOrientedBlockMeta;
use crate::meta::AbstractBlockMeta;
use crate::meta::SegmentInfo;
use crate::meta::Statistics;
pub trait AbstractSegment: Send + Sync + 'static {
    fn blocks(&self) -> Box<dyn Iterator<Item = Arc<dyn AbstractBlockMeta>> + '_>;
    fn summary(&self) -> Statistics;
    fn to_bytes(&self) -> Result<Vec<u8>>;
}

impl AbstractSegment for SegmentInfo {
    fn blocks(&self) -> Box<dyn Iterator<Item = Arc<dyn AbstractBlockMeta>> + '_> {
        Box::new(
            self.blocks
                .iter()
                .map(|b| b.clone() as Arc<dyn AbstractBlockMeta>),
        )
    }

    fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes()
    }

    fn summary(&self) -> Statistics {
        self.summary.clone()
    }
}

pub struct ColumnOrientedSegment {
    /// blocks belong to this segment
    pub block_metas: RecordBatch,
    /// summary statistics
    pub summary: Statistics,
}

impl AbstractSegment for ColumnOrientedSegment {
    fn blocks(&self) -> Box<dyn Iterator<Item = Arc<(dyn AbstractBlockMeta + 'static)>>> {
        Box::new(BlockMetaIter::new(self.block_metas.clone()))
    }

    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut write_buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut write_buffer, self.block_metas.schema(), None)?;
        writer.write(&self.block_metas)?;
        let _ = writer.close()?;
        Ok(write_buffer)
    }

    fn summary(&self) -> Statistics {
        self.summary.clone()
    }
}

pub struct BlockMetaIter {
    blocks: RecordBatch,
    index: usize,
}

impl BlockMetaIter {
    pub fn new(blocks: RecordBatch) -> Self {
        Self { blocks, index: 0 }
    }
}

impl Iterator for BlockMetaIter {
    type Item = Arc<dyn AbstractBlockMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.blocks.num_rows() {
            return None;
        }
        let index = self.index;
        self.index += 1;
        Some(Arc::new(ColumnOrientedBlockMeta::new(
            self.blocks.clone(),
            index,
        )))
    }
}
