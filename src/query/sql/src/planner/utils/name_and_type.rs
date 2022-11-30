// Copyright 2022 Datafuse Labs.
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

use std::ops::Index;
use common_datavalues::{DataField, DataSchemaRef, DataSchemaRefExt, DataTypeImpl};

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NameAndDataTypes(Vec<NameAndDataType>);

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NameAndDataType {
    pub name: String,
    pub data_type: DataTypeImpl,
}

impl NameAndDataType {
    pub fn new(name: impl Into<String>, data_type: DataTypeImpl) -> Self {
        NameAndDataType {
            name: name.into(),
            data_type,
        }
    }

    pub fn as_data_field(&self) -> DataField {
        DataField::new(self.name.as_str(), self.data_type.clone())
    }
}

impl NameAndDataTypes {
    pub fn new(name_and_types: Vec<NameAndDataType>) -> Self {
        NameAndDataTypes(name_and_types)
    }

    /// Get iterator of inner `Vec<NameAndDataType>`
    pub fn iter(&self) -> std::slice::Iter<'_, NameAndDataType> {
        self.0.iter()
    }

    /// Get mutable reference of inner `Vec<NameAndDataType>`
    pub fn inner_mut(&mut self) -> &mut Vec<NameAndDataType> {
        &mut self.0
    }

    /// Get reference of inner `Vec<NameAndDataType>`
    pub fn inner(&self) -> &Vec<NameAndDataType> {
        &self.0
    }

    /// Get vector index of first `NameAndDataType` with `name`
    pub fn index_of_name(&self, name: impl AsRef<str>) -> Option<usize> {
        self.0.iter().position(|x| x.name == name.as_ref())
    }
}

impl Index<usize> for NameAndDataTypes {
    type Output = NameAndDataType;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IntoIterator for NameAndDataTypes {
    type Item = NameAndDataType;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<&DataSchemaRef> for NameAndDataTypes {
    fn from(schema: &DataSchemaRef) -> Self {
        let name_and_types = schema.fields().iter().map(|f| {
            NameAndDataType::new(f.name().to_string(), f.data_type().clone())
        }).collect();
        NameAndDataTypes::new(name_and_types)
    }
}

// TODO(leiysky): we won't allow to convert `NameAndDataTypes` to `DataSchemaRef`
// with new expression, deprecate this function then.
pub fn to_data_schema(
    name_and_data_types: &NameAndDataTypes,
) -> DataSchemaRef {
    let fields = name_and_data_types
        .iter()
        .map(|name_and_data_type| name_and_data_type.as_data_field())
        .collect::<Vec<_>>();
    DataSchemaRefExt::create(fields)
}
