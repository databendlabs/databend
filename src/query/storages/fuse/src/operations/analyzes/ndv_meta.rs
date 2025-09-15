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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::ColumnId;
use databend_common_storage::MetaHLL;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::RawBlockMeta;
use databend_storages_common_table_meta::meta::Statistics;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct AnalyzeExtraMeta {
    pub row_count: u64,
    pub column_hlls: HashMap<ColumnId, MetaHLL>,
}

impl Debug for AnalyzeExtraMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("AnalyzeExtraMeta").finish()
    }
}

#[typetag::serde(name = "analyze_collect_ndv")]
impl BlockMetaInfo for AnalyzeExtraMeta {}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct AnalyzeSegmentMeta {
    pub segment_location: Location,
    pub blocks: RawBlockMeta,
    pub origin_summary: Statistics,

    pub raw_block_hlls: Vec<RawBlockHLL>,
    pub block_hll_indexes: Vec<usize>,
    pub merged_hlls: HashMap<ColumnId, MetaHLL>,
}

impl AnalyzeSegmentMeta {
    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.segment_location.0.hash(&mut s);
        s.finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub enum AnalyzeNDVMeta {
    Segment(AnalyzeSegmentMeta),
    Extras(AnalyzeExtraMeta),
}

impl AnalyzeNDVMeta {
    pub fn to_part(self) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(self))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&AnalyzeNDVMeta> {
        info.as_any()
            .downcast_ref::<AnalyzeNDVMeta>()
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast from PartInfo to AnalyzeNDVMeta."))
    }
}

impl Debug for AnalyzeNDVMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("AnalyzeNDVMeta").finish()
    }
}

#[typetag::serde(name = "analyze_ndv_meta")]
impl BlockMetaInfo for AnalyzeNDVMeta {}

#[typetag::serde(name = "analyze_ndv_part")]
impl PartInfo for AnalyzeNDVMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<AnalyzeNDVMeta>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        match self {
            Self::Extras(_) => 0,
            Self::Segment(info) => info.hash(),
        }
    }
}
