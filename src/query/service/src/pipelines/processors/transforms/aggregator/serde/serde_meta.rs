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

use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;

pub const BUCKET_TYPE: usize = 1;
pub const SPILLED_TYPE: usize = 2;

// Cannot change to enum, because bincode cannot deserialize custom enum
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct AggregateSerdeMeta {
    pub typ: usize,
    pub bucket: isize,
    pub location: Option<String>,
    pub columns_layout: Vec<usize>,
}

impl AggregateSerdeMeta {
    pub fn create(bucket: isize) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: BUCKET_TYPE,
            bucket,
            location: None,
            columns_layout: vec![],
        })
    }

    pub fn create_spilled(
        bucket: isize,
        location: String,
        columns_layout: Vec<usize>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta {
            typ: SPILLED_TYPE,
            bucket,
            columns_layout,
            location: Some(location),
        })
    }
}

#[typetag::serde(name = "aggregate_serde")]
impl BlockMetaInfo for AggregateSerdeMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match AggregateSerdeMeta::downcast_ref_from(info) {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
