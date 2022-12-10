// Copyright 2022 Datafuse Labs.
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
use common_storages_table_meta::meta::ClusterStatistics;

use crate::pruning::BlockIndex;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct DeletionPartInfo {
    pub index: BlockIndex,
    pub cluster_stats: Option<ClusterStatistics>,
    pub inner_part: PartInfoPtr,
}

#[typetag::serde(name = "deletion")]
impl PartInfo for DeletionPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<DeletionPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        self.inner_part.hash()
    }
}

impl DeletionPartInfo {
    pub fn create(
        index: BlockIndex,
        cluster_stats: Option<ClusterStatistics>,
        inner_part: PartInfoPtr,
    ) -> PartInfoPtr {
        Arc::new(Box::new(DeletionPartInfo {
            index,
            cluster_stats,
            inner_part,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&DeletionPartInfo> {
        match info.as_any().downcast_ref::<DeletionPartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to DeletionPartInfo.",
            )),
        }
    }
}
