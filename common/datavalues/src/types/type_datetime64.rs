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

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DateTime64Type {
    /// The time resolution is determined by the precision parameter, range from 0 to 9
    /// Typically are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).
    precision: usize,
    /// tz indicates the timezone, if it's None, it's UTC.
    tz: Option<String>,
}

impl DateTime64Type {
    pub fn create(precision: usize, tz: Option<String>) -> Self {
        DateTime64Type { precision, tz }
    }
    pub fn arc(precision: usize, tz: Option<String>) -> DataTypePtr {
        Arc::new(DateTime64Type { precision, tz })
    }

    pub fn tz(&self) -> Option<&String> {
        self.tz.as_ref()
    }

    pub fn precision(&self) -> usize {
        self.precision
    }

    #[inline]
    pub fn utc_timestamp(&self, v: i64) -> DateTime<Utc> {
        let v = v * 10_i64.pow(9 - self.precision as u32);

        // ns
        Utc.timestamp(v / 1_000_000_000, (v % 1_000_000_000) as u32)
    }

    #[inline]
    pub fn seconds(&self, v: i64) -> i64 {
        let v = v * 10_i64.pow(9 - self.precision as u32);
        v / 1_000_000_000
    }

    pub fn format_string(&self) -> String {
        if self.precision == 0 {
            "%Y-%m-%d %H:%M:%S".to_string()
        } else {
            format!("%Y-%m-%d %H:%M:%S%.{}f", self.precision)
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
        DataValue::Int64(0)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_i64()?;
        let column = Series::from_data(&[value]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_i64())
            .collect::<Result<Vec<_>>>()?;

        Ok(Series::from_data(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Int64
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "DateTime64".to_string());
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        mp.insert(
            ARROW_EXTENSION_META.to_string(),
            format!("{}{}", self.precision, tz),
        );
        Some(mp)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        Box::new(DateTimeSerializer::<i64>::create(
            tz.parse::<Tz>().unwrap(),
            self.precision as u32,
        ))
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        Box::new(DateTimeDeserializer::<i64> {
            builder: MutablePrimitiveColumn::<i64>::with_capacity(capacity),
            tz: tz.parse::<Tz>().unwrap(),
        })
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i64>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for DateTime64Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name(), self.precision())
    }
}
