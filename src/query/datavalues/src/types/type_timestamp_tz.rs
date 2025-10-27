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

use std::hash::Hash;

use crate::DataType;
use crate::TypeID;

/// Timestamp type only stores UTC time in microseconds
#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct TimestampTzType;

impl DataType for TimestampTzType {
    fn data_type_id(&self) -> TypeID {
        TypeID::TimestampTz
    }

    fn name(&self) -> String {
        "TimestampTz".to_string()
    }
}

impl std::fmt::Debug for TimestampTzType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TimestampTz")
    }
}
