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
use common_storages_table_meta::meta::BlockMeta;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone)]
pub enum CompactTask {
    Trival(Arc<BlockMeta>),
    Normal(Vec<Arc<BlockMeta>>),
}

impl CompactTask {
    pub fn get_block_metas(&self) -> Vec<Arc<BlockMeta>> {
        match self {
            CompactTask::Trival(block_meta) => vec![block_meta.clone()],
            CompactTask::Normal(block_metas) => block_metas.clone(),
        }
    }

    fn is_trival(&self) -> bool {
        matches!(self, CompactTask::Trival(_))
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CompactPartInfo {
    pub tasks: Vec<CompactTask>,
    pub order: usize,
}

#[typetag::serde(name = "compact")]
impl PartInfo for CompactPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<CompactPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl CompactPartInfo {
    pub fn create(tasks: Vec<CompactTask>, order: usize) -> PartInfoPtr {
        Arc::new(Box::new(CompactPartInfo { tasks, order }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&CompactPartInfo> {
        match info.as_any().downcast_ref::<CompactPartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to CompactPartInfo.",
            )),
        }
    }

    pub fn is_all_trivial(&self) -> bool {
        self.tasks.iter().all(|v| v.is_trival())
    }
}
