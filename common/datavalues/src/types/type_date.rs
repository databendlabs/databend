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
use common_exception::ErrorCode;
use common_exception::Result;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

/// date ranges from 1000-01-01 to 9999-12-31
/// date_max and date_min means days offset from 1970-01-01
/// any date not in the range will be invalid
pub const DATE_MAX: i32 = 2932896;
pub const DATE_MIN: i32 = -354285;

#[inline]
pub fn check_date(days: i32) -> Result<()> {
    if days >= DATE_MIN && days <= DATE_MAX {
        return Ok(());
    }
    Err(ErrorCode::InvalidDate(
        "Date only ranges from 1000-01-01 to 9999-12-31",
    ))
}

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateType {}

impl DateType {
    pub fn arc() -> DataTypePtr {
        Arc::new(Self {})
    }
}

#[typetag::serde]
impl DataType for DateType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Date
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "Date".to_string()
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
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "Date".to_string());
        Some(mp)
    }

    fn create_serializer(&self) -> TypeSerializerImpl {
        DateSerializer::<i32>::default().into()
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        DateDeserializer::<i32> {
            builder: MutablePrimitiveColumn::<i32>::with_capacity(capacity),
        }
        .into()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i32>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for DateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
