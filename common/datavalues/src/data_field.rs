// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_macros::MallocSizeOf;

use crate::remove_nullable;
use crate::types::data_type::from_arrow_field;
use crate::types::data_type::DataTypePtr;
use crate::wrap_nullable;
use crate::TypeID;

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Clone, MallocSizeOf)]
pub struct DataField {
    name: String,
    /// default_expr is serialized representation from PlanExpression
    default_expr: Option<Vec<u8>>,
    #[ignore_malloc_size_of = "insignificant"]
    data_type: DataTypePtr,
}

impl DataField {
    pub fn new(name: &str, data_type: DataTypePtr) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
        }
    }

    pub fn new_nullable(name: &str, data_type: DataTypePtr) -> Self {
        let data_type = wrap_nullable(&data_type);
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type,
        }
    }

    #[must_use]
    pub fn with_default_expr(mut self, default_expr: Option<Vec<u8>>) -> Self {
        self.default_expr = default_expr;
        self
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataTypePtr {
        &self.data_type
    }

    pub fn default_expr(&self) -> &Option<Vec<u8>> {
        &self.default_expr
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

    pub fn to_arrow(&self) -> ArrowField {
        self.data_type.to_arrow_field(&self.name)
    }
}

impl From<&ArrowField> for DataField {
    fn from(f: &ArrowField) -> Self {
        let dt: DataTypePtr = from_arrow_field(f);

        DataField::new(f.name(), dt)
    }
}

impl std::fmt::Debug for DataField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("DataField");
        let remove_nullable = remove_nullable(self.data_type());
        debug_struct
            .field("name", &self.name)
            .field("data_type", &remove_nullable)
            .field("nullable", &self.is_nullable());
        if let Some(ref default_expr) = self.default_expr {
            debug_struct.field(
                "default_expr",
                &String::from_utf8(default_expr.to_owned()).unwrap(),
            );
        }
        debug_struct.finish()
    }
}

impl std::fmt::Display for DataField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
