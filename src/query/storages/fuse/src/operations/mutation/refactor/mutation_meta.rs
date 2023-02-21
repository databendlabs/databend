//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoPtr;
use serde::Deserializer;
use serde::Serializer;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::Location;

#[derive(Clone, Debug)]
pub enum Mutation {
    DoNothing,
    Replaced(Location, u64),
    Deleted,
}

#[derive(Clone, Debug)]
pub struct MutationSourceMeta {
    pub index: BlockMetaIndex,
    pub op: Mutation,
}

impl serde::Serialize for MutationSourceMeta {
    fn serialize<S>(&self, _: S) -> common_exception::Result<S::Ok, S::Error>
    where S: Serializer {
        unimplemented!("Unimplemented serialize MutationSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for MutationSourceMeta {
    fn deserialize<D>(_: D) -> common_exception::Result<Self, D::Error>
    where D: Deserializer<'de> {
        unimplemented!("Unimplemented deserialize MutationSourceMeta")
    }
}

#[typetag::serde(name = "mutation_transform_meta")]
impl BlockMetaInfo for MutationSourceMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals MutationSourceMeta")
    }
}

impl MutationSourceMeta {
    pub fn create(index: BlockMetaIndex, op: Mutation) -> BlockMetaInfoPtr {
        Box::new(MutationSourceMeta { index, op })
    }

    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&MutationSourceMeta> {
        match info.as_any().downcast_ref::<MutationSourceMeta>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from BlockMetaInfo to MutationSourceMeta.",
            )),
        }
    }
}
