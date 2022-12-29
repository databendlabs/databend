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

use common_datablocks::BlockMetaInfo;
use common_datablocks::BlockMetaInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;

use crate::pruning::BlockIndex;

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct UpdateSourceMeta {
    pub index: BlockIndex,
    pub replace: Arc<BlockMeta>,
}

#[typetag::serde(name = "update_source_meta")]
impl BlockMetaInfo for UpdateSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<UpdateSourceMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl UpdateSourceMeta {
    pub fn create(index: BlockIndex, replace: Arc<BlockMeta>) -> BlockMetaInfoPtr {
        Arc::new(Box::new(UpdateSourceMeta { index, replace }))
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&UpdateSourceMeta> {
        match info.as_any().downcast_ref::<UpdateSourceMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to UpdateSourceMeta.",
            )),
        }
    }
}
