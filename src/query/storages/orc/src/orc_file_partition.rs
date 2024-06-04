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
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq)]
pub struct OrcFilePartition {
    pub path: String,
    pub size: usize,
}

#[typetag::serde(name = "orc_part")]
impl PartInfo for OrcFilePartition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<OrcFilePartition>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.path.hash(&mut s);
        s.finish()
    }
}

impl OrcFilePartition {
    pub fn from_part(info: &PartInfoPtr) -> Result<&OrcFilePartition> {
        info.as_any()
            .downcast_ref::<OrcFilePartition>()
            .ok_or_else(|| {
                ErrorCode::Internal("Cannot downcast from PartInfo to OrcFilePartition.")
            })
    }
}
