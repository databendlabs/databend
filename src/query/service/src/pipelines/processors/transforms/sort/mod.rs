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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::SortSpillParams;
use enum_as_inner::EnumAsInner;
use sort_spill::SpillableBlock;

use crate::spillers::Spiller;

mod bounds;
mod merge_sort;
mod sort_broadcast;
mod sort_builder;
mod sort_collect;
mod sort_exchange_injector;
mod sort_merge_stream;
mod sort_restore;
mod sort_route;
mod sort_spill;
#[cfg(test)]
mod test_memory;

pub use merge_sort::*;
pub use sort_broadcast::*;
pub use sort_builder::*;
pub use sort_collect::*;
pub use sort_exchange_injector::*;
pub use sort_merge_stream::*;
pub use sort_restore::*;
pub use sort_route::*;

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
    sequences: Vec<Box<[SpillableBlock]>>,
}

local_block_meta_serde!(SortCollectedMeta);

#[typetag::serde(name = "sort_collected")]
impl BlockMetaInfo for SortCollectedMeta {}

trait RowsStat {
    fn total_rows(&self) -> usize;

    fn in_memory_rows(&self) -> usize;
}

impl RowsStat for Vec<DataBlock> {
    fn total_rows(&self) -> usize {
        self.iter().map(|b| b.num_rows()).sum::<usize>()
    }

    fn in_memory_rows(&self) -> usize {
        self.total_rows()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SortExchangeMeta {
    params: SortSpillParams,
    bounds: Bounds,
}

#[typetag::serde(name = "sort_exchange")]
impl BlockMetaInfo for SortExchangeMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        SortExchangeMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SortBound {
    index: u32,
    next: SortBoundNext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, EnumAsInner)]
pub enum SortBoundNext {
    More,
    Next(u32),
    Skip,
    Last,
}

impl SortBound {
    fn create(index: u32, next: SortBoundNext) -> Box<dyn BlockMetaInfo> {
        debug_assert!(index != u32::MAX);
        SortBound { index, next }.boxed()
    }
}

#[typetag::serde(name = "sort_bound")]
impl BlockMetaInfo for SortBound {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        SortBound::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(*self)
    }
}
