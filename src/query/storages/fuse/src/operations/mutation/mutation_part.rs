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

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::ClusterStatistics;

use crate::pruning::DeletedSegmentInfo;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub enum Mutation {
    MutationDeletedSegment(MutationDeletedSegment),
    MutationPartInfo(MutationPartInfo),
}

#[typetag::serde(name = "mutation_enum")]
impl PartInfo for Mutation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match self {
            Self::MutationDeletedSegment(mutation_deleted_segment) => {
                mutation_deleted_segment.equals(info)
            }
            Self::MutationPartInfo(mutation_part_info) => mutation_part_info.equals(info),
        }
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
            .ok_or(ErrorCode::Internal(
                "Cannot downcast from PartInfo to Mutation.",
            ))
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct MutationDeletedSegment {
    pub deleted_segment: DeletedSegmentInfo,
}

#[typetag::serde(name = "mutation_delete_segment")]
impl PartInfo for MutationDeletedSegment {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<MutationDeletedSegment>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        self.deleted_segment.hash()
    }
}

impl MutationDeletedSegment {
    pub fn create(deleted_segment: DeletedSegmentInfo) -> Self {
        MutationDeletedSegment { deleted_segment }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct MutationPartInfo {
    pub index: BlockMetaIndex,
    pub cluster_stats: Option<ClusterStatistics>,
    pub inner_part: PartInfoPtr,
    pub whole_block_mutation: bool,
}

#[typetag::serde(name = "mutation")]
impl PartInfo for MutationPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<MutationPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        self.inner_part.hash()
    }
}

impl MutationPartInfo {
    pub fn create(
        index: BlockMetaIndex,
        cluster_stats: Option<ClusterStatistics>,
        inner_part: PartInfoPtr,
        whole_block_mutation: bool,
    ) -> Self {
        MutationPartInfo {
            index,
            cluster_stats,
            inner_part,
            whole_block_mutation,
        }
    }
}
