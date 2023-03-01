// Copyright 2023 Datafuse Labs.
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum AggregateSerdeMeta {
    Bucket(isize),
    Spilled {
        bucket: isize,
        location: String,
        columns_layout: Vec<usize>,
    },
}

impl AggregateSerdeMeta {
    pub fn create(bucket: isize) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta::Bucket(bucket))
    }

    pub fn create_spilled(
        bucket: isize,
        location: String,
        columns_layout: Vec<usize>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateSerdeMeta::Spilled {
            bucket,
            location,
            columns_layout,
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
