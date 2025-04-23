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

mod sort_exchange;
mod sort_merge;
mod sort_sample;
mod sort_wait;

use std::sync::Arc;

use bounds::Bounds;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::SortSpillParams;
pub use sort_merge::*;
pub use sort_sample::*;

mod builder;
pub use builder::TransformSortBuilder;

mod bounds;
mod collect;
mod exchange;
mod execute;
mod merge_sort;
mod sort_spill;
mod wait;

use sort_spill::SpillableBlock;

use crate::spillers::Spiller;

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

trait MemoryRows {
    fn in_memory_rows(&self) -> usize;
}

impl MemoryRows for Vec<DataBlock> {
    fn in_memory_rows(&self) -> usize {
        self.iter().map(|s| s.num_rows()).sum::<usize>()
    }
}
