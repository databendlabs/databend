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

use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::DataType;
use super::type_id::TypeID;
pub use crate::prelude::*;
use crate::TypeDeserializer;
use crate::TypeSerializer;

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct BooleanType {}

impl BooleanType {
    pub fn arc() -> DataTypePtr {
        Arc::new(Self {})
    }
}

#[typetag::serde]
impl DataType for BooleanType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Boolean
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "Boolean"
    }

    fn default_value(&self) -> DataValue {
        DataValue::Boolean(false)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_bool()?;
        let column = Series::from_data(&[value]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_bool())
            .collect::<Result<Vec<bool>>>()?;

        Ok(Series::from_data(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Boolean
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(BooleanSerializer {})
    }
    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(BooleanDeserializer {
            builder: MutableBooleanColumn::with_capacity(capacity),
        })
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutableBooleanColumn::with_capacity(capacity))
    }
}

impl std::fmt::Debug for BooleanType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
