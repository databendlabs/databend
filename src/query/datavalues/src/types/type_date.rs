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

// SQL DATE bounds, as days since 1970-01-01. Keep in sync with common-expression.
// Out-of-range SQL values are errors, not silently replaced with another date.
pub const DATE_MAX: i32 = 3_298_504; // 11000-12-31
pub const DATE_MIN: i32 = -719_162; // 0001-01-01

#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct DateType {}

impl DateType {
    pub fn new_impl() -> DataTypeImpl {
        DataTypeImpl::Date(Self {})
    }
}

impl DataType for DateType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Date
    }

    fn name(&self) -> String {
        "Date".to_string()
    }
}

impl std::fmt::Debug for DateType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
