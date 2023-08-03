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
use storages_common_table_meta::meta::BlockMeta;

use crate::operations::common::BlockMetaIndex;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct CompactPartInfo {
    pub blocks: Vec<Arc<BlockMeta>>,
    pub index: BlockMetaIndex,
}

#[typetag::serde(name = "compact")]
impl PartInfo for CompactPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<CompactPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        0
    }
}

impl CompactPartInfo {
    pub fn create(blocks: Vec<Arc<BlockMeta>>, index: BlockMetaIndex) -> PartInfoPtr {
        Arc::new(Box::new(CompactPartInfo { blocks, index }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&CompactPartInfo> {
        info.as_any()
            .downcast_ref::<CompactPartInfo>()
            .ok_or(ErrorCode::Internal(
                "Cannot downcast from PartInfo to CompactPartInfo.",
            ))
    }
}
