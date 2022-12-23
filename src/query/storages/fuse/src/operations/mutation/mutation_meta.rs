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
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::Statistics;

use crate::operations::mutation::AbortOperation;

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct MutationMeta {
    pub segments: Vec<Location>,
    pub summary: Statistics,
    pub abort_operation: AbortOperation,
}

#[typetag::serde(name = "mutation_meta")]
impl BlockMetaInfo for MutationMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<MutationMeta>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl MutationMeta {
    pub fn create(
        segments: Vec<Location>,
        summary: Statistics,
        abort_operation: AbortOperation,
    ) -> BlockMetaInfoPtr {
        Arc::new(Box::new(MutationMeta {
            segments,
            summary,
            abort_operation,
        }))
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&MutationMeta> {
        match info.as_any().downcast_ref::<MutationMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to MutationMeta.",
            )),
        }
    }
}
