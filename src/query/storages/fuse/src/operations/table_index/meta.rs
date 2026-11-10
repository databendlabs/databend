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

//! Internal refresh plans and per-block pipeline contributions.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::sync::Arc;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
use databend_common_meta_app::schema::TableIndex;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::SpatialIndexMeta;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::StatisticsOfVectorColumns;

use crate::io::BlockReader;
use crate::io::SpatialIndexResult;
use crate::io::block_index::PendingVectorIndex;
use crate::io::block_index::WrittenInvertedIndex;
use crate::operations::BlockMetaIndex;
use crate::operations::table_index::inverted_index::RefreshInvertedIndex;

/// One index's definition, table-schema positions and prepared builder state.
pub struct IndexSpec {
    pub index: TableIndex,
    pub fields: Vec<usize>,
    pub schema: TableSchemaRef,
    pub inverted: Option<RefreshInvertedIndex>,
}

/// Shared input for all index branches of one selected block.
pub struct RefreshBlock {
    pub index: BlockMetaIndex,
    pub meta: Arc<BlockMeta>,
    pub existing: ExistingIndexes,
    pub hlls: Option<RawBlockHLL>,
    pub top_n: Option<BlockTopN>,
    pub missing: HashSet<String>,
    pub projection: Vec<usize>,
    pub reader: Arc<BlockReader>,
}

/// Shared container metadata is read once per block, not once per index.
pub struct ExistingIndexes {
    pub bloom: Option<Arc<BloomIndexMeta>>,
    pub spatial: Option<Arc<SpatialIndexMeta>>,
}

#[derive(Clone)]
pub struct RefreshInput(pub Arc<RefreshBlock>);
impl Debug for RefreshInput {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.write_str("RefreshInput")
    }
}
local_block_meta_serde!(RefreshInput);
#[typetag::serde(name = "refresh_block_input")]
impl BlockMetaInfo for RefreshInput {
    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

pub struct BuiltSpatialIndex {
    pub columns: SpatialIndexResult,
    pub stats: Option<StatisticsOfSpatialColumns>,
    pub ids: Vec<ColumnId>,
    pub name: String,
}

pub struct WrittenVectorIndex {
    pub metas: Vec<BlockIndexMeta>,
    pub statistics: Option<StatisticsOfVectorColumns>,
}

pub enum IndexArtifact {
    Inverted(WrittenInvertedIndex),
    PendingVector(PendingVectorIndex),
    Vector(WrittenVectorIndex),
    Bloom(BloomIndex),
    Spatial(BuiltSpatialIndex),
}

pub struct BuiltIndexes {
    pub plan: Arc<RefreshBlock>,
    pub artifacts: Vec<IndexArtifact>,
}
impl Debug for BuiltIndexes {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.write_str("BuiltIndexes")
    }
}
local_block_meta_serde!(BuiltIndexes);
#[typetag::serde(name = "built_refresh_indexes")]
impl BlockMetaInfo for BuiltIndexes {}

/// Prepared indexes and selected blocks from one snapshot. No additional reads or
/// snapshot ownership are hidden behind the execution pipeline.
pub struct RefreshPlan {
    pub specs: Vec<Arc<IndexSpec>>,
    pub blocks: VecDeque<Arc<RefreshBlock>>,
}
