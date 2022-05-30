//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_datablocks::DataBlock;

use crate::sessions::QueryContext;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::FuseTable;

pub enum Deletion {
    NothingDeleted,
    Remains(DataBlock),
}

#[allow(dead_code)]
pub struct Replacement {
    original_block_loc: Location,
    new_block_loc: Location,
}

pub type SegmentIndex = usize;

pub struct DeletionCollector {
    mutations: HashMap<SegmentIndex, Vec<Replacement>>,
}

impl DeletionCollector {
    pub fn new(table: &FuseTable, ctx: &QueryContext) -> Self {
        todo!()
    }

    ///Replaces the block located at `block_meta.location`,
    /// of segment indexed by `seg_idx`, with a new block `r`
    pub async fn replace_with(
        &mut self,
        seg_idx: usize,
        block_location: &Location,
        replace_with: DataBlock,
    ) -> common_exception::Result<()> {
        // write new block, and keep the mutations
        let new_block_loc = self.write_new_block(replace_with).await?;
        let original_block_loc = block_location.clone();
        self.mutations
            .entry(seg_idx)
            .or_default()
            .push(Replacement {
                original_block_loc,
                new_block_loc,
            });
        Ok(())
    }

    async fn write_new_block(&self, block: DataBlock) -> common_exception::Result<Location> {
        todo!()
    }
}
