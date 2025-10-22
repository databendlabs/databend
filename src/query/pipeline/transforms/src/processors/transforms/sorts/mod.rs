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

use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use enum_as_inner::EnumAsInner;
pub use sort_broadcast::*;
pub use sort_collect::*;
pub use sort_k_way_merge::*;
pub use sort_local_merge::*;
pub use sort_merge::*;
pub use sort_merge_base::*;
pub use sort_merge_limit::*;
pub use sort_merge_stream::*;
pub use sort_multi_merge::*;
pub use sort_partial::*;
pub use sort_restore::*;
pub use sort_route::*;
pub use sort_spill::*;

pub use self::core::utils;
use self::core::Bounds;

pub mod core;
mod sort_broadcast;
mod sort_collect;
mod sort_k_way_merge;
mod sort_local_merge;
mod sort_merge;
mod sort_merge_base;
mod sort_merge_limit;
mod sort_merge_stream;
mod sort_multi_merge;
mod sort_partial;
mod sort_restore;
mod sort_route;
mod sort_spill;

#[derive(Clone)]
pub struct Base<S: Send + Clone> {
    pub schema: DataSchemaRef,
    pub spiller: S,
    pub sort_row_offset: usize,
    pub limit: Option<usize>,
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
    pub index: u32,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SortTaskMeta {
    id: usize,
    total: usize,
    input: usize,
}

#[typetag::serde(name = "sort_task")]
impl BlockMetaInfo for SortTaskMeta {}
