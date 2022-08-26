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

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;
use rand::prelude::*;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;
use crate::serializations::TimestampSerializer;
use crate::serializations::TypeSerializerImpl;

/// timestamp ranges from 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999
/// timestamp_max and timestamp_min means days offset from 1970-01-01 00:00:00.000000
/// any timestamp not in the range will be invalid
pub const TIMESTAMP_MAX: i64 = 253402300799999999;
pub const TIMESTAMP_MIN: i64 = -30610224000000000;
pub const MICROSECONDS: i64 = 1_000_000;

#[inline]
pub fn check_timestamp(micros: i64) -> Result<()> {
    if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&micros) {
        return Ok(());
    }
    Err(ErrorCode::InvalidTimestamp(
        "Timestamp only ranges from 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999",
    ))
}

/// Timestamp type only stores UTC time in microseconds
#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct TimestampType {
    /// Typically are used - 0 (seconds) 3 (milliseconds), 6 (microseconds)
    precision: usize,
}

impl TimestampType {
    pub fn create(precision: usize) -> Self {
        TimestampType { precision }
    }

    pub fn new_impl(precision: usize) -> DataTypeImpl {
        DataTypeImpl::Timestamp(TimestampType { precision })
    }

    pub fn precision(&self) -> usize {
        self.precision
    }

    #[inline]
    pub fn utc_timestamp(&self, v: i64) -> DateTime<Utc> {
        Utc.timestamp(v / 1_000_000, (v % 1_000_000 * 1000) as u32)
    }

    #[inline]
    pub fn to_seconds(&self, v: i64) -> i64 {
        v / 1_000_000
    }

    pub fn format_string(&self) -> String {
        if self.precision == 0 {
            "%Y-%m-%d %H:%M:%S".to_string()
        } else {
            format!("%Y-%m-%d %H:%M:%S%.{}f", self.precision)
        }
    }
}

impl DataType for TimestampType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Timestamp
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        format!("Timestamp({})", self.precision)
    }

    fn aliases(&self) -> &[&str] {
        match self.precision {
            0 => &["DateTime(0)"],
            1 => &["DateTime(1)"],
            2 => &["DateTime(2)"],
            3 => &["DateTime(3)"],
            4 => &["DateTime(4)"],
            5 => &["DateTime(5)"],
            6 => &["Timestamp", "DateTime", "DateTime(6)"],
            _ => &[],
        }
    }

    fn default_value(&self) -> DataValue {
        DataValue::Int64(0)
    }

    fn random_value(&self) -> DataValue {
        let mut rng = rand::rngs::SmallRng::from_entropy();
        let ts = rng.gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX);
        DataValue::Int64(ts)
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

    // To avoid the overhead of precision conversion, we store Microsecond for all precisions.
    fn arrow_type(&self) -> ArrowType {
        ArrowType::Timestamp(TimeUnit::Microsecond, None)
    }

    fn create_serializer_inner<'a>(&self, col: &'a ColumnRef) -> Result<TypeSerializerImpl<'a>> {
        Ok(TimestampSerializer::<'a>::try_create(self.precision, col)?.into())
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        TimestampDeserializer {
            builder: MutablePrimitiveColumn::<i64>::with_capacity(capacity),
            precision: self.precision,
        }
        .into()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i64>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for TimestampType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timestamp({})", self.precision())
    }
}
