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
use super::data_type::DataTypeImpl;
use super::type_id::TypeID;

#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct StructType {
    names: Option<Vec<String>>,
    types: Vec<DataTypeImpl>,
}

impl StructType {
    pub fn new_impl(names: Option<Vec<String>>, types: Vec<DataTypeImpl>) -> DataTypeImpl {
        DataTypeImpl::Struct(Self::create(names, types))
    }

    pub fn create(names: Option<Vec<String>>, types: Vec<DataTypeImpl>) -> Self {
        if let Some(ref names) = names {
            debug_assert!(names.len() == types.len());
        }
        StructType { names, types }
    }

    pub fn names(&self) -> &Option<Vec<String>> {
        &self.names
    }

    pub fn types(&self) -> &Vec<DataTypeImpl> {
        &self.types
    }
}

impl DataType for StructType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Struct
    }

    fn name(&self) -> String {
        let mut type_name = String::new();
        type_name.push_str("Struct(");
        let mut first = true;
        match &self.names {
            Some(names) => {
                for (name, ty) in names.iter().zip(self.types.iter()) {
                    if !first {
                        type_name.push_str(", ");
                    }
                    first = false;
                    type_name.push_str(name);
                    type_name.push(' ');
                    type_name.push_str(&ty.name());
                }
            }
            None => {
                for ty in self.types.iter() {
                    if !first {
                        type_name.push_str(", ");
                    }
                    first = false;
                    type_name.push_str(&ty.name());
                }
            }
        }
        type_name.push(')');

        type_name
    }

    fn can_inside_nullable(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for StructType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
