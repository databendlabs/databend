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

use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct RuntimeFilterMeta {
    pub node_id: String,
    pub need_to_build: bool,
}

impl RuntimeFilterMeta {
    pub fn create(node_id: String, need_to_build: bool) -> BlockMetaInfoPtr {
        Box::new(RuntimeFilterMeta {
            node_id,
            need_to_build,
        })
    }
}

impl Debug for RuntimeFilterMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("RuntimeFilterMeta")
            .field("node_id", &self.node_id)
            .field("need_to_build", &self.need_to_build)
            .finish()
    }
}

#[typetag::serde(name = "runtime_filter_meta")]
impl BlockMetaInfo for RuntimeFilterMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        Self::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
