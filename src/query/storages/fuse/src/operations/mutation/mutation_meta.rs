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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;

use crate::operations::mutation::AbortOperation;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct SerializeDataMeta {
    pub index: BlockMetaIndex,
    pub cluster_stats: Option<ClusterStatistics>,
}

#[typetag::serde(name = "serialize_data_meta")]
impl BlockMetaInfo for SerializeDataMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match SerializeDataMeta::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl SerializeDataMeta {
    pub fn create(
        index: BlockMetaIndex,
        cluster_stats: Option<ClusterStatistics>,
    ) -> BlockMetaInfoPtr {
        Box::new(SerializeDataMeta {
            index,
            cluster_stats,
        })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&SerializeDataMeta> {
        match SerializeDataMeta::downcast_ref_from(info) {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to  SerializeDataMeta.",
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    DoNothing,
    Replaced(Arc<BlockMeta>),
    Deleted,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct MutationTransformMeta {
    pub index: BlockMetaIndex,
    pub op: Mutation,
}

#[typetag::serde(name = "mutation_transform_meta")]
impl BlockMetaInfo for MutationTransformMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match MutationTransformMeta::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl MutationTransformMeta {
    pub fn create(index: BlockMetaIndex, op: Mutation) -> BlockMetaInfoPtr {
        Box::new(MutationTransformMeta { index, op })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&MutationTransformMeta> {
        match MutationTransformMeta::downcast_ref_from(info) {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to MutationTransformMeta.",
            )),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct MutationSinkMeta {
    pub segments: Vec<Location>,
    pub summary: Statistics,
    pub abort_operation: AbortOperation,
}

#[typetag::serde(name = "mutation_sink_meta")]
impl BlockMetaInfo for MutationSinkMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match MutationSinkMeta::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl MutationSinkMeta {
    pub fn create(
        segments: Vec<Location>,
        summary: Statistics,
        abort_operation: AbortOperation,
    ) -> BlockMetaInfoPtr {
        Box::new(MutationSinkMeta {
            segments,
            summary,
            abort_operation,
        })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&MutationSinkMeta> {
        match MutationSinkMeta::downcast_ref_from(info) {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to MutationSinkMeta.",
            )),
        }
    }
}
