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

use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::plan::StreamColumnMeta;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ReclusterSourceMeta {
    pub need_local_sort: bool,
    pub inner: Option<BlockMetaInfoPtr>,
}

impl ReclusterSourceMeta {
    pub fn create(inner: Option<BlockMetaInfoPtr>, need_local_sort: bool) -> BlockMetaInfoPtr {
        Box::new(Self {
            need_local_sort,
            inner,
        })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Option<&ReclusterSourceMeta> {
        Self::downcast_ref_from(info)
            .or_else(|| {
                StreamColumnMeta::downcast_ref_from(info)
                    .and_then(|meta| meta.inner.as_ref())
                    .and_then(Self::from_meta)
            })
            .or_else(|| {
                InternalColumnMeta::downcast_ref_from(info)
                    .and_then(|meta| meta.inner.as_ref())
                    .and_then(Self::from_meta)
            })
    }
}

#[typetag::serde(name = "recluster_source_meta")]
impl BlockMetaInfo for ReclusterSourceMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        ReclusterSourceMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
