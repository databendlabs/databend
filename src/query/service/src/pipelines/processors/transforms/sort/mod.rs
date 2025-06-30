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

use bounds::Bounds;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::SortSpillParams;
use sort_spill::SpillableBlock;

use crate::spillers::Spiller;

mod bounds;
mod merge_sort;
mod sort_builder;
mod sort_collect;
mod sort_combine;
mod sort_exchange;
mod sort_exchange_injector;
mod sort_execute;
mod sort_route;
mod sort_shuffle;
mod sort_spill;

pub use sort_builder::*;
pub use sort_exchange::*;
pub use sort_route::*;
pub use sort_shuffle::*;

#[derive(Clone)]
struct Base {
    schema: DataSchemaRef,
    spiller: Arc<Spiller>,
    sort_row_offset: usize,
    limit: Option<usize>,
}

#[derive(Debug)]
struct SortCollectedMeta {
    params: SortSpillParams,
    bounds: Bounds,
    blocks: Vec<Box<[SpillableBlock]>>,
}

local_block_meta_serde!(SortCollectedMeta);

#[typetag::serde(name = "sort_collected")]
impl BlockMetaInfo for SortCollectedMeta {}

#[derive(Debug)]
struct SortScatteredMeta(pub Vec<Option<SortCollectedMeta>>);

local_block_meta_serde!(SortScatteredMeta);

#[typetag::serde(name = "sort_scattered")]
impl BlockMetaInfo for SortScatteredMeta {}

trait MemoryRows {
    fn in_memory_rows(&self) -> usize;
}

impl MemoryRows for Vec<DataBlock> {
    fn in_memory_rows(&self) -> usize {
        self.iter().map(|s| s.num_rows()).sum::<usize>()
    }
}
