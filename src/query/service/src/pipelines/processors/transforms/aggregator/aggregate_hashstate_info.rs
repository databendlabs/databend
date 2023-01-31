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
use common_expression::BlockMetaInfoPtr;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::pipelines::processors::transforms::group_by::ArenaHolder;

#[derive(Debug)]
pub struct AggregateHashStateInfo {
    pub bucket: usize,
    // a subhashtable state
    pub hash_state: Box<dyn Any + Send + Sync>,
    pub state_holder: Option<ArenaHolder>,
}

impl AggregateHashStateInfo {
    pub fn create(
        bucket: usize,
        hash_state: Box<dyn Any + Send + Sync>,
        state_holder: Option<ArenaHolder>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateHashStateInfo {
            bucket,
            hash_state,
            state_holder,
        })
    }
}

impl Serialize for AggregateHashStateInfo {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        unreachable!("AggregateHashStateInfo does not support exchanging between multiple nodes")
    }
}

impl<'de> Deserialize<'de> for AggregateHashStateInfo {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        unreachable!("AggregateHashStateInfo does not support exchanging between multiple nodes")
    }
}

#[typetag::serde(name = "aggregate_hash_state_info")]
impl BlockMetaInfo for AggregateHashStateInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for AggregateHashStateInfo")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for AggregateHashStateInfo")
    }
}
