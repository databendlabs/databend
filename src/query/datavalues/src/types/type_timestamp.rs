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

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

/// Timestamp type only stores UTC time in microseconds
#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct TimestampType {
    // Deprecated, used as a placeholder for backward compatibility
    #[serde(skip)]
    precision: usize,
}

impl TimestampType {
    pub fn new_impl() -> DataTypeImpl {
        DataTypeImpl::Timestamp(TimestampType { precision: 0 })
    }

    #[inline]
    pub fn to_seconds(&self, v: i64) -> i64 {
        v / 1_000_000
    }

    pub fn format_string(&self) -> &str {
        "%Y-%m-%d %H:%M:%S%.6f"
    }
}

impl DataType for TimestampType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Timestamp
    }

    fn name(&self) -> String {
        "Timestamp".to_string()
    }
}

impl std::fmt::Debug for TimestampType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Timestamp")
    }
}
