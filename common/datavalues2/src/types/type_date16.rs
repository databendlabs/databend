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
pub struct Date16Type {}

impl Date16Type {
    pub fn arc() -> DataTypePtr {
        Arc::new(Date16Type {})
    }
}

#[typetag::serde]
impl DataType for Date16Type {
    fn data_type_id(&self) -> TypeID {
        TypeID::Date16
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "Date16"
    }

    fn aliases(&self) -> &[&str] {
        &["Date"]
    }

    fn default_value(&self) -> DataValue {
        DataValue::UInt64(0)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_u64()?;

        let column = Series::from_data(&[value as u16]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_u64())
            .collect::<Result<Vec<_>>>()?;
        let value = value.iter().map(|v| *v as u16).collect::<Vec<_>>();
        Ok(Series::from_data(value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::UInt16
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "Date16".to_string());
        Some(mp)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateSerializer::<u16>::default())
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(DateDeserializer::<u16> {
            builder: MutablePrimitiveColumn::<u16>::with_capacity(capacity),
        })
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<u16>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for Date16Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
