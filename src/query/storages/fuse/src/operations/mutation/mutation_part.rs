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
use std::sync::Arc;

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::ClusterStatistics;

use crate::pruning::DeletedSegmentInfo;

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
        match info.as_any().downcast_ref::<MutationDeletedSegment>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        self.deleted_segment.hash()
    }
}

impl MutationDeletedSegment {
    pub fn create(deleted_segment: DeletedSegmentInfo) -> Self {
        MutationDeletedSegment {
            deleted_segment: deleted_segment,
        }
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&MutationDeletedSegment> {
        match info.as_any().downcast_ref::<MutationDeletedSegment>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to MutationDeletedSegment.",
            )),
        }
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
        match info.as_any().downcast_ref::<MutationPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
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
    ) -> PartInfoPtr {
        Arc::new(Box::new(MutationPartInfo {
            index,
            cluster_stats,
            inner_part,
            whole_block_mutation,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&MutationPartInfo> {
        match info.as_any().downcast_ref::<MutationPartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to MutationPartInfo.",
            )),
        }
    }
}
