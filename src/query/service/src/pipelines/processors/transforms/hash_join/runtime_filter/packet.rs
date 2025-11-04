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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::Scalar;

/// Represents a runtime filter that can be transmitted and merged.
///
/// # Fields
///
/// * `id` - Unique identifier for each runtime filter, corresponds one-to-one with `(build key, probe key)` pair
/// * `inlist` - Deduplicated list of build key column
/// * `min_max` - The min and max values of the build column
/// * `bloom` - The deduplicated hashes of the build column
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, PartialEq)]
pub struct RuntimeFilterPacket {
    pub id: usize,
    pub inlist: Option<Column>,
    pub min_max: Option<SerializableDomain>,
    pub bloom: Option<HashSet<u64>>,
}

impl Debug for RuntimeFilterPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RuntimeFilterPacket {{ id: {}, inlist: {:?}, min_max: {:?}, bloom: {:?} }}",
            self.id,
            self.inlist,
            self.min_max,
            self.bloom.is_some()
        )
    }
}

/// Represents a collection of runtime filter packets that correspond to a join operator.
///
/// # Fields
///
/// * `packets` - A map of runtime filter packets, keyed by their unique identifier `RuntimeFilterPacket::id`. When `packets` is `None`, it means that `build_num_rows` is zero.
/// * `build_rows` - Total number of rows used when building the runtime filters.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct JoinRuntimeFilterPacket {
    #[serde(default)]
    pub packets: Option<HashMap<usize, RuntimeFilterPacket>>,
    #[serde(default)]
    pub build_rows: usize,
}

#[typetag::serde(name = "join_runtime_filter_packet")]
impl BlockMetaInfo for JoinRuntimeFilterPacket {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        JoinRuntimeFilterPacket::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct SerializableDomain {
    pub min: Scalar,
    pub max: Scalar,
}
