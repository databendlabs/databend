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

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;

/// Memory table lazy partition information.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct MemoryPartInfo {}

#[typetag::serde(name = "memory_part")]
impl PartInfo for MemoryPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<MemoryPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        0
    }
}

impl MemoryPartInfo {
    pub fn create() -> PartInfoPtr {
        Arc::new(Box::new(MemoryPartInfo {}))
    }
}
