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
pub struct DateTimeType {
    /// The time resolution is determined by the precision parameter, range from 0 to 9
    /// Typically are used - 0 (seconds) 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).
    precision: usize,
    /// tz indicates the timezone, if it's None, it's UTC.
    tz: Option<String>,
}

impl DateTimeType {
    pub fn create(precision: usize, tz: Option<String>) -> Self {
        DateTimeType { precision, tz }
    }

    pub fn arc(precision: usize, tz: Option<String>) -> DataTypePtr {
        Arc::new(DateTimeType { precision, tz })
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
    pub fn to_seconds(&self, v: i64) -> i64 {
        let v = v * 10_i64.pow(9 - self.precision as u32);
        v / 1_000_000_000
    }

    #[inline]
    pub fn from_nano_seconds(&self, v: i64) -> i64 {
        v / 10_i64.pow(9 - self.precision as u32)
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
impl DataType for DateTimeType {
    fn data_type_id(&self) -> TypeID {
        TypeID::DateTime
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        let c = match self.precision {
            0 => "DateTime",
            1 => "DateTime(1)",
            2 => "DateTime(2)",
            3 => "DateTime(3)",
            4 => "DateTime(4)",
            5 => "DateTime(5)",
            6 => "DateTime(6)",
            7 => "DateTime(7)",
            8 => "DateTime(8)",
            9 => "DateTime(9)",
            _ => unreachable!(),
        };
        c.to_string()
    }

    fn aliases(&self) -> &[&str] {
        &[]
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
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "DateTime".to_string());
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        mp.insert(
            ARROW_EXTENSION_META.to_string(),
            format!("{}{}", self.precision, tz),
        );
        Some(mp)
    }

    fn create_serializer(&self) -> TypeSerializerImpl {
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        DateTimeSerializer::<i64>::create(tz.parse::<Tz>().unwrap(), self.precision as u32).into()
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        DateTimeDeserializer::<i64> {
            builder: MutablePrimitiveColumn::<i64>::with_capacity(capacity),
            tz: tz.parse::<Tz>().unwrap(),
            precision: self.precision,
        }
        .into()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i64>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for DateTimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.precision() == 0 {
            write!(f, "DateTime")
        } else {
            write!(f, "DateTime({})", self.precision())
        }
    }
}
