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
use common_arrow::arrow::datatypes::Field;

use super::data_type::DataTypePtr;
use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DataTypeList {
    name: String,
    size: usize,
    inner: DataTypePtr,
}

impl DataTypeList {
    pub fn create(name: String, size: usize, inner: DataTypePtr) -> Self {
        DataTypeList { name, size, inner }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn inner_type(&self) -> &DataTypePtr {
        &self.inner
    }
}

#[typetag::serde]
impl IDataType for DataTypeList {
    fn type_id(&self) -> TypeID {
        TypeID::List
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_value(&self) -> DataValue {
        let c: Vec<DataValue> = (0..self.size).map(|_| self.inner.default_value()).collect();
        DataValue::List(c)
    }

    fn arrow_type(&self) -> ArrowType {
        let field = Field::new(&self.name, self.inner.arrow_type(), false);
        ArrowType::List(Box::new(field))
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        todo!()
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        todo!()
    }
}
