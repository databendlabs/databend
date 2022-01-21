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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::ops::Deref;

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::fmt::Formatter;
use common_macros::MallocSizeOf;

use crate::DataType;

#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Hash, Eq, PartialOrd, Ord, MallocSizeOf,
)]
pub struct DataTypeAndNullable {
    data_type: DataType,
    nullable: bool,
}

impl DataTypeAndNullable {
    pub fn create(data_type: &DataType, nullable: bool) -> DataTypeAndNullable {
        DataTypeAndNullable {
            nullable,
            data_type: data_type.clone(),
        }
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl Deref for DataTypeAndNullable {
    type Target = DataType;

    fn deref(&self) -> &Self::Target {
        &self.data_type
    }
}

impl Display for DataTypeAndNullable {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self.nullable {
            true => write!(f, "Nullable({})", self.data_type),
            false => write!(f, "{}", self.data_type),
        }
    }
}

impl Debug for DataTypeAndNullable {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DataTypeAndNullable")
            .field("data_type", &self.data_type)
            .field("nullable", &self.nullable)
            .finish()
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Hash, Eq, PartialOrd, Ord, MallocSizeOf,
)]
pub struct DataField {
    name: String,
    /// default_expr is serialized representation from PlanExpression
    default_expr: Option<Vec<u8>>,
    data_type_and_nullable: DataTypeAndNullable,
}

impl DataField {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        DataField {
            name: name.to_string(),
            default_expr: None,
            data_type_and_nullable: DataTypeAndNullable {
                data_type,
                nullable,
            },
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

    pub fn default_expr(&self) -> &Option<Vec<u8>> {
        &self.default_expr
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type_and_nullable.data_type
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.data_type_and_nullable.nullable
    }

    #[inline]
    pub fn data_type_and_nullable(&self) -> &DataTypeAndNullable {
        &self.data_type_and_nullable
    }

    /// Check to see if `self` is a superset of `other` field. Superset is defined as:
    ///
    /// * if nullability doesn't match, self needs to be nullable
    /// * self.metadata is a superset of other.metadata
    /// * all other fields are equal
    pub fn contains(&self, other: &DataField) -> bool {
        if self.name != other.name || self.data_type() != other.data_type() {
            return false;
        }

        if self.is_nullable() != other.is_nullable() && !self.is_nullable() {
            return false;
        }

        true
    }

    pub fn to_arrow(&self) -> ArrowField {
        let custom_name = match self.data_type() {
            DataType::Date16 => Some("Date16"),
            DataType::Date32 => Some("Date32"),
            DataType::DateTime32(_) => Some("DateTime32"),
            DataType::DateTime64(_, _) => Some("DateTime64"),
            _ => None,
        };

        let custom_metadata = match self.data_type() {
            DataType::DateTime32(tz) => tz.clone(),
            _ => None,
        };

        let nullable = self.data_type_and_nullable.nullable;
        let arrow_type = self.data_type_and_nullable.data_type.to_arrow();
        let mut f = ArrowField::new(&self.name, arrow_type, nullable);
        if let Some(custom_name) = custom_name {
            let mut mp = BTreeMap::new();
            mp.insert(
                "ARROW:extension:databend_name".to_string(),
                custom_name.to_string(),
            );

            if let Some(m) = custom_metadata {
                mp.insert("ARROW:extension:databend_metadata".to_string(), m);
            }
            f = f.with_metadata(mp);
        }

        f
    }
}

impl From<&ArrowField> for DataField {
    fn from(f: &ArrowField) -> Self {
        let mut dt: DataType = f.data_type().into();
        if let Some(m) = f.metadata() {
            if let Some(custom_name) = m.get("ARROW:extension:databend_name") {
                let metatada = m.get("ARROW:extension:databend_metadata");
                match custom_name.as_str() {
                    "Date16" => dt = DataType::Date16,
                    "Date32" => dt = DataType::Date32,
                    "DateTime32" => dt = DataType::DateTime32(metatada.cloned()),
                    "DateTime64" => dt = DataType::DateTime64(3, metatada.cloned()),
                    _ => {}
                }
            }
        }
        DataField::new(f.name(), dt, f.is_nullable())
    }
}

impl std::fmt::Debug for DataField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("DataField");
        debug_struct
            .field("name", &self.name)
            .field("data_type", self.data_type())
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
