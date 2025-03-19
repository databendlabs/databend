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

use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;

use crate::meta::column_oriented_segment::block_read_info::BlockReadInfo;
use crate::meta::BlockMeta;
pub trait AbstractBlockMeta: Send + Sync + 'static + Sized {
    fn read_info(&self) -> BlockReadInfo;
}

impl AbstractBlockMeta for BlockMeta {
    fn read_info(&self) -> BlockReadInfo {
        BlockReadInfo {
            location: self.location.0.clone(),
            row_count: self.row_count,
            col_metas: self.col_metas.clone(),
            compression: self.compression,
            block_size: self.block_size,
        }
    }
}

impl AbstractBlockMeta for ColumnOrientedBlockMetaIter {
    fn read_info(&self) -> BlockReadInfo {
        todo!()
    }
}

#[derive(Clone)]
pub struct ColumnOrientedBlockMetaIter {
    block_metas: DataBlock,
    row_number: usize,
    schema: TableSchemaRef, // TODO(Sky): cache field index
}

impl ColumnOrientedBlockMetaIter {
    pub fn new(block_metas: DataBlock, schema: TableSchemaRef) -> Self {
        Self {
            block_metas,
            row_number: 0,
            schema,
        }
    }
}

impl Iterator for ColumnOrientedBlockMetaIter {
    type Item = Self;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_number >= self.block_metas.num_rows() {
            None
        } else {
            let item = self.clone();
            self.row_number += 1;
            Some(item)
        }
    }
}
