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
use std::collections::HashSet;

use common_exception::ErrorCode;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::Scalar;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum MergeIntoOperation {
    Delete(DeletionByColumn),
    None,
}

pub type UniqueKeyDigest = u128;
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DeletionByColumn {
    // used in table meta level pruning
    pub columns_min_max: Vec<(Scalar, Scalar)>,
    // used in block level
    pub key_hashes: HashSet<UniqueKeyDigest>,
}

#[typetag::serde(name = "merge_into_operation_meta")]
impl BlockMetaInfo for MergeIntoOperation {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        match info.as_any().downcast_ref::<MergeIntoOperation>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl TryFrom<DataBlock> for MergeIntoOperation {
    type Error = ErrorCode;

    fn try_from(value: DataBlock) -> Result<Self, Self::Error> {
        let meta = value.get_owned_meta().ok_or_else(|| {
            ErrorCode::Internal(
                "convert MergeIntoOperation from data block failed, no block meta found",
            )
        })?;
        MergeIntoOperation::downcast_from(meta).ok_or_else(|| {
            ErrorCode::Internal(
                "downcast block meta to MutationIntoOperation failed, type mismatch",
            )
        })
    }
}
