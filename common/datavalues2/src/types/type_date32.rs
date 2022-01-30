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
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct Date32Type {}

impl Date32Type {
    pub fn arc() -> DataTypePtr {
        Arc::new(Self {})
    }
}

#[typetag::serde]
impl DataType for Date32Type {
    fn data_type_id(&self) -> TypeID {
        TypeID::Date32
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "Date32"
    }

    fn default_value(&self) -> DataValue {
        DataValue::Int64(0)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_i64()?;
        let column = Series::from_data(&[value as i32]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_i64())
            .collect::<Result<Vec<_>>>()?;

        let value = value.iter().map(|v| *v as i32).collect::<Vec<_>>();
        Ok(Series::from_data(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Int32
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "Date32".to_string());
        Some(mp)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateSerializer::<i32>::default())
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(DateDeserializer::<i32> {
            builder: MutablePrimitiveColumn::<i32>::with_capacity(capacity),
        })
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i32>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for Date32Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
