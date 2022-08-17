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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use crate::prelude::*;

pub struct TypeFactory {
    // types used by type conversion functions
    conversion_types: HashSet<String>,
    case_insensitive_types: HashMap<String, DataTypeImpl>,
}

static TYPE_FACTORY: Lazy<Arc<TypeFactory>> = Lazy::new(|| {
    let mut type_factory = TypeFactory::create();

    type_factory.register(NullType::new_impl());
    type_factory.register(BooleanType::new_impl());
    type_factory.register(StringType::new_impl());

    type_factory.register(UInt8Type::new_impl());
    type_factory.register(UInt16Type::new_impl());
    type_factory.register(UInt32Type::new_impl());
    type_factory.register(UInt64Type::new_impl());

    type_factory.register(Int8Type::new_impl());
    type_factory.register(Int16Type::new_impl());
    type_factory.register(Int32Type::new_impl());
    type_factory.register(Int64Type::new_impl());

    type_factory.register(Float32Type::new_impl());
    type_factory.register(Float64Type::new_impl());

    type_factory.register(DateType::new_impl());
    type_factory.register(VariantType::new_impl());
    type_factory.register(VariantArrayType::new_impl());
    type_factory.register(VariantObjectType::new_impl());

    // Timestamp is a special case
    {
        for precision in 0..7 {
            type_factory.register(TimestampType::new_impl(precision));
        }
    }

    Arc::new(type_factory)
});

impl TypeFactory {
    pub fn create() -> Self {
        Self {
            conversion_types: HashSet::new(),
            case_insensitive_types: HashMap::new(),
        }
    }

    pub fn instance() -> &'static TypeFactory {
        TYPE_FACTORY.as_ref()
    }

    pub fn get(&self, name: impl AsRef<str>) -> Result<DataTypeImpl> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();

        let is_nullable =
            lowercase_name.ends_with(" null") || lowercase_name.starts_with("nullable(");

        if is_nullable {
            if lowercase_name.ends_with(" null") {
                let name = lowercase_name[..lowercase_name.len() - 5].to_string();
                return self.get(name).map(NullableType::new_impl);
            } else {
                let name = lowercase_name[9..lowercase_name.len() - 1].to_string();
                return self.get(name).map(NullableType::new_impl);
            }
        }

        if lowercase_name.starts_with("array(") {
            let name = lowercase_name[6..lowercase_name.len() - 1].to_string();
            return self.get(name).map(ArrayType::new_impl);
        }

        if lowercase_name.starts_with("tuple(") {
            let names = origin_name[6..origin_name.len() - 1].to_string();
            let bytes = names.as_bytes();

            let mut start1 = 0;
            let mut start2 = 0;
            let mut index = 0;
            let mut inner_names = Vec::new();
            let mut inner_data_types = Vec::new();
            while index <= bytes.len() {
                if index == bytes.len() || bytes[index] == b',' {
                    if start1 != start2 {
                        inner_names.push(names[start1..start2 - 1].to_string());
                    }
                    inner_data_types.push(self.get(&names[start2..index])?);
                } else if bytes[index] == b'(' {
                    let mut depth = 1;
                    let mut sub_index = index + 1;
                    while sub_index < bytes.len() {
                        if bytes[sub_index] == b'(' {
                            depth += 1;
                        } else if bytes[sub_index] == b')' {
                            depth -= 1;
                        }
                        if depth == 0 {
                            break;
                        }
                        sub_index += 1;
                    }
                    if depth != 0 {
                        return Result::Err(ErrorCode::IllegalDataType(
                            "The names of tuple elements format error",
                        ));
                    }
                    if index >= 5
                        && (names[index - 5..index].to_lowercase() == "tuple"
                            || names[index - 5..index].to_lowercase() == "array")
                    {
                        if start1 != start2 {
                            inner_names.push(names[start1..start2 - 1].to_string());
                        }
                        inner_data_types.push(self.get(&names[index - 5..sub_index + 1])?);
                    } else {
                        return Result::Err(ErrorCode::IllegalDataType(
                            "The names of tuple elements format error",
                        ));
                    }
                    index = sub_index + 1;
                    if index == bytes.len() {
                        break;
                    } else if bytes[index] == b',' {
                        index += 1;
                        continue;
                    }
                } else if bytes[index] == b' ' {
                    if index > 0 && bytes[index - 1] == b',' {
                        start1 = index + 1;
                        start2 = index + 1;
                    } else {
                        start2 = index + 1;
                    }
                }
                index += 1;
            }
            if inner_data_types.is_empty() {
                return Result::Err(ErrorCode::IllegalDataType("Tuple cannot be empty"));
            }
            if inner_names.is_empty() {
                return Ok(StructType::new_impl(None, inner_data_types));
            } else {
                if inner_names.len() != inner_data_types.len() {
                    return Result::Err(ErrorCode::IllegalDataType(
                        "The named tuple elements must have name",
                    ));
                }
                let mut names_set = HashSet::with_capacity(inner_names.len());
                for inner_name in &inner_names {
                    if !names_set.insert(inner_name) {
                        return Result::Err(ErrorCode::IllegalDataType(
                            "The names of tuple elements must be unique",
                        ));
                    }
                }
                return Ok(StructType::new_impl(Some(inner_names), inner_data_types));
            }
        }

        self.case_insensitive_types
            .get(&lowercase_name)
            .cloned()
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(format!("Unsupported data type: {}", origin_name))
            })
    }

    pub fn register_names(&self) -> Vec<&str> {
        self.case_insensitive_types
            .keys()
            .map(|s| s.as_str())
            .collect()
    }

    pub fn register(&mut self, data_type: DataTypeImpl) {
        let mut names = vec![data_type.name()];

        for alias in data_type.aliases() {
            names.push(alias.to_string());
        }
        for name in names {
            self.case_insensitive_types
                .insert(name.to_lowercase(), data_type.clone());
            self.conversion_types.insert(name);
        }
    }

    pub fn conversion_names(&self) -> Vec<&str> {
        self.conversion_types.iter().map(|s| s.as_str()).collect()
    }
}
