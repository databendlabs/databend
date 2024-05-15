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

use crate::remove_nullable;
use crate::types::data_type::DataType;
use crate::types::data_type::DataTypeImpl;
use crate::wrap_nullable;
use crate::TypeID;

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Clone)]
pub struct DataField {
    name: String,
    /// default_expr is serialized representation from PlanExpression
    default_expr: Option<String>,
    data_type: DataTypeImpl,
}

impl DataField {
    pub fn new(name: &str, data_type: DataTypeImpl) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
        }
    }

    pub fn new_nullable(name: &str, data_type: DataTypeImpl) -> Self {
        let data_type = wrap_nullable(&data_type);
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
        }
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<String>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataTypeImpl {
        &self.data_type
    }

    pub fn default_expr(&self) -> Option<&String> {
        self.default_expr.as_ref()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type.is_nullable()
    }

    #[inline]
    pub fn is_nullable_or_null(&self) -> bool {
        self.data_type.is_nullable() || self.data_type.data_type_id() == TypeID::Null
    }

    /// Check to see if `self` is a superset of `other` field. Superset is defined as:
    ///
    /// * if nullability doesn't match, self needs to be nullable
    /// * self.metadata is a superset of other.metadata
    /// * all other fields are equal
    pub fn contains(&self, other: &DataField) -> bool {
        if self.name != other.name
            || self.data_type().data_type_id() != other.data_type().data_type_id()
        {
            return false;
        }

        if self.is_nullable() != other.is_nullable() && !self.is_nullable() {
            return false;
        }

        true
    }
}

impl std::fmt::Debug for DataField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("DataField");
        let remove_nullable = remove_nullable(self.data_type());
        debug_struct
            .field("name", &self.name)
            .field("data_type", &remove_nullable.data_type_id())
            .field("nullable", &self.is_nullable());
        if let Some(ref default_expr) = self.default_expr {
            debug_struct.field("default_expr", default_expr);
        }
        debug_struct.finish()
    }
}

impl std::fmt::Display for DataField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
