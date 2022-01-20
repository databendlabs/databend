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

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use chrono_tz::Tz;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::DataType;
use super::data_type::ARROW_EXTENSION_META;
use super::data_type::ARROW_EXTENSION_NAME;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateTime64Type {
    precision: usize,
    tz: Option<String>,
}

impl DateTime64Type {
    pub fn create(precision: usize, tz: Option<String>) -> Self {
        DateTime64Type { precision, tz }
    }
    pub fn arc(precision: usize, tz: Option<String>) -> DataTypePtr {
        Arc::new(DateTime64Type { precision, tz })
    }
    pub fn precision(&self) -> usize {
        self.precision
    }

    #[inline]
    pub fn utc_timestamp(&self, v: u64) -> DateTime<Utc> {
        if self.precision <= 3 {
            // ms
            Utc.timestamp(v as i64 / 1000, (v % 1000 * 1_000_000) as u32)
        } else {
            // ns
            Utc.timestamp(v as i64 / 1_000_000_000_000, (v % 1_000_000_000_000) as u32)
        }
    }
}

#[typetag::serde]
impl DataType for DateTime64Type {
    fn data_type_id(&self) -> TypeID {
        TypeID::DateTime64
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "DateTime64"
    }

    fn default_value(&self) -> DataValue {
        DataValue::UInt64(0)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_u64()?;
        let column = Series::from_data(&[value]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_u64())
            .collect::<Result<Vec<_>>>()?;

        Ok(Series::from_data(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::UInt64
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "DateTime64".to_string());
        if let Some(tz) = &self.tz {
            mp.insert(ARROW_EXTENSION_META.to_string(), tz.to_string());
        }
        Some(mp)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateTimeSerializer::<u64>::default())
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        Box::new(DateTimeDeserializer::<u64> {
            builder: MutablePrimitiveColumn::<u64>::with_capacity(capacity),
            tz: tz.parse::<Tz>().unwrap(),
        })
    }
}
