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
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Statistics;

use crate::operations::mutation::SegmentIndex;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub enum Mutation {
    MutationDeletedSegment(DeletedSegmentInfo),
    MutationPartInfo(MutationPartInfo),
}

#[typetag::serde(name = "mutation_enum")]
impl PartInfo for Mutation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<Mutation>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        match self {
            Self::MutationDeletedSegment(mutation_deleted_segment) => {
                mutation_deleted_segment.hash()
            }
            Self::MutationPartInfo(mutation_part_info) => mutation_part_info.hash(),
        }
    }
}

impl Mutation {
    pub fn from_part(info: &PartInfoPtr) -> Result<&Mutation> {
        info.as_any()
            .downcast_ref::<Mutation>()
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast from PartInfo to Mutation."))
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct DeletedSegmentInfo {
    /// segment index.
    pub index: SegmentIndex,
    /// segment statistics.
    pub summary: Statistics,
}

impl DeletedSegmentInfo {
    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.index.hash(&mut s);
        s.finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct MutationPartInfo {
    pub index: BlockMetaIndex,
    pub cluster_stats: Option<ClusterStatistics>,
    pub inner_part: PartInfoPtr,
    pub whole_block_mutation: bool,
}

impl MutationPartInfo {
    fn hash(&self) -> u64 {
        self.inner_part.hash()
    }
}
