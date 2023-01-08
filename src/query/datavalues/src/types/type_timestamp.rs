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

use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

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
    // Deprecated, used as a placeholder for backward compatibility
    #[serde(skip)]
    precision: usize,
}

impl TimestampType {
    pub fn new_impl() -> DataTypeImpl {
        DataTypeImpl::Timestamp(TimestampType { precision: 0 })
    }

    #[inline]
    pub fn utc_timestamp(&self, v: i64) -> DateTime<Utc> {
        Utc.timestamp(v / 1_000_000, (v % 1_000_000 * 1000) as u32)
    }

    #[inline]
    pub fn to_seconds(&self, v: i64) -> i64 {
        v / 1_000_000
    }

    pub fn format_string(&self) -> &str {
        "%Y-%m-%d %H:%M:%S%.6f"
    }
}

impl DataType for TimestampType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Timestamp
    }

    fn name(&self) -> String {
        "Timestamp".to_string()
    }

    // To avoid the overhead of precision conversion, we store Microsecond for all precisions.
    fn arrow_type(&self) -> ArrowType {
        ArrowType::Timestamp(TimeUnit::Microsecond, None)
    }
}

impl std::fmt::Debug for TimestampType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timestamp")
    }
}
