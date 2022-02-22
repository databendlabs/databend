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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use crate::prelude::*;

pub struct TypeFactory {
    case_insensitive_types: HashMap<String, DataTypePtr>,
}

static TYPE_FACTORY: Lazy<Arc<TypeFactory>> = Lazy::new(|| {
    let mut type_factory = TypeFactory::create();

    type_factory.register(NullType::arc());
    type_factory.register(BooleanType::arc());
    type_factory.register(StringType::arc());

    type_factory.register(UInt8Type::arc());
    type_factory.register(UInt16Type::arc());
    type_factory.register(UInt32Type::arc());
    type_factory.register(UInt64Type::arc());

    type_factory.register(Int8Type::arc());
    type_factory.register(Int16Type::arc());
    type_factory.register(Int32Type::arc());
    type_factory.register(Int64Type::arc());

    type_factory.register(Float32Type::arc());
    type_factory.register(Float64Type::arc());

    type_factory.register(Date16Type::arc());
    type_factory.register(Date32Type::arc());
    type_factory.register(DateTime32Type::arc(None));
    type_factory.register(DateTime64Type::arc(3, None));

    type_factory.add_array_wrapper();
    type_factory.add_nullable_wrapper();

    Arc::new(type_factory)
});

impl TypeFactory {
    pub fn create() -> Self {
        Self {
            case_insensitive_types: HashMap::new(),
        }
    }

    pub fn instance() -> &'static TypeFactory {
        TYPE_FACTORY.as_ref()
    }

    pub fn get(&self, name: impl AsRef<str>) -> Result<&DataTypePtr> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();
        self.case_insensitive_types
            .get(&lowercase_name)
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(format!("Unsupported data_type: {}", origin_name))
            })
    }

    pub fn register(&mut self, data_type: DataTypePtr) {
        let mut names = vec![data_type.name()];

        for alias in data_type.aliases() {
            names.push(alias);
        }
        for name in names {
            self.case_insensitive_types
                .insert(name.to_lowercase(), data_type.clone());
        }
    }

    pub fn add_array_wrapper(&mut self) {
        let mut arrays = HashMap::new();
        for (k, v) in self.case_insensitive_types.iter() {
            let data_type: DataTypePtr = Arc::new(ArrayType::create(v.clone()));
            arrays.insert(
                format!("Array({})", k).to_ascii_lowercase(),
                data_type.clone(),
            );
        }
        self.case_insensitive_types.extend(arrays);
    }

    pub fn add_nullable_wrapper(&mut self) {
        let mut nulls = HashMap::new();
        for (k, v) in self.case_insensitive_types.iter() {
            if v.can_inside_nullable() {
                let data_type: DataTypePtr = Arc::new(NullableType::create(v.clone()));
                nulls.insert(
                    format!("Nullable({})", k).to_ascii_lowercase(),
                    data_type.clone(),
                );
            }
        }
        self.case_insensitive_types.extend(nulls);
    }
}
