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
use common_exception::ErrorCode;

use super::data_type::DataTypePtr;
use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DataTypeStruct {
    names: Vec<String>,
    types: Vec<DataTypePtr>,
}

impl DataTypeStruct {
    pub fn create(names: Vec<String>, types: Vec<DataTypePtr>) -> Self {
        DataTypeStruct { names, types }
    }

    pub fn names(&self) -> &Vec<String> {
        &self.names
    }

    pub fn types(&self) -> &Vec<DataTypePtr> {
        &self.types
    }
}

#[typetag::serde]
impl IDataType for DataTypeStruct {
    fn data_type_id(&self) -> TypeID {
        TypeID::Struct
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_value(&self) -> DataValue {
        let c: Vec<DataValue> = self.types.iter().map(|t| t.default_value()).collect();
        DataValue::Struct(c)
    }

    fn create_constant_column(
        &self,
        data: &DataValue,
        size: usize,
    ) -> common_exception::Result<ColumnRef> {
        if let DataValue::Struct(value) = data {
            todo!()
        }
        return Result::Err(ErrorCode::BadDataValueType(format!(
            "Unexpected type:{:?} to generate list column",
            data.value_type()
        )));
    }

    fn arrow_type(&self) -> ArrowType {
        let fields = self
            .names
            .iter()
            .zip(self.types.iter())
            .map(|(name, type_)| type_.to_arrow_field(name))
            .collect();

        ArrowType::Struct(fields)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        todo!()
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        todo!()
    }
}
