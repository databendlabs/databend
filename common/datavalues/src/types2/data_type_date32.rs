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

use common_arrow::arrow::datatypes::DataType as ArrowType;

use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DataTypeDate32 {}

#[typetag::serde]
impl IDataType for DataTypeDate32 {
    fn type_id(&self) -> TypeID {
        TypeID::Int32
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Int32
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateSerializer::<i32>::default())
    }
    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(DateDeserializer::<i32> {
            builder: PrimitiveArrayBuilder::<i32>::with_capacity(capacity),
        })
    }
}
