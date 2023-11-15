// Copyright 2020-2022 Jorge C. Leitão
// Copyright 2021 Datafuse Labs
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

use parquet2::schema::types::PrimitiveLogicalType;
use parquet2::schema::types::TimeUnit as ParquetTimeUnit;
use parquet2::statistics::PrimitiveStatistics;
use parquet2::statistics::Statistics as ParquetStatistics;
use parquet2::types::NativeType as ParquetNativeType;

use crate::arrow::array::*;
use crate::arrow::datatypes::TimeUnit;
use crate::arrow::error::Result;
use crate::arrow::types::NativeType;

pub fn timestamp(logical_type: Option<&PrimitiveLogicalType>, time_unit: TimeUnit, x: i64) -> i64 {
    let unit = if let Some(PrimitiveLogicalType::Timestamp { unit, .. }) = logical_type {
        unit
    } else {
        return x;
    };

    match (unit, time_unit) {
        (ParquetTimeUnit::Milliseconds, TimeUnit::Second) => x / 1_000,
        (ParquetTimeUnit::Microseconds, TimeUnit::Second) => x / 1_000_000,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Second) => x * 1_000_000_000,

        (ParquetTimeUnit::Milliseconds, TimeUnit::Millisecond) => x,
        (ParquetTimeUnit::Microseconds, TimeUnit::Millisecond) => x / 1_000,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Millisecond) => x / 1_000_000,

        (ParquetTimeUnit::Milliseconds, TimeUnit::Microsecond) => x * 1_000,
        (ParquetTimeUnit::Microseconds, TimeUnit::Microsecond) => x,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Microsecond) => x / 1_000,

        (ParquetTimeUnit::Milliseconds, TimeUnit::Nanosecond) => x * 1_000_000,
        (ParquetTimeUnit::Microseconds, TimeUnit::Nanosecond) => x * 1_000,
        (ParquetTimeUnit::Nanoseconds, TimeUnit::Nanosecond) => x,
    }
}

pub(super) fn push<P: ParquetNativeType, T: NativeType, F: Fn(P) -> Result<T> + Copy>(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
    map: F,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<T>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<T>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<PrimitiveStatistics<P>>().unwrap());
    min.push(from.and_then(|s| s.min_value.map(map)).transpose()?);
    max.push(from.and_then(|s| s.max_value.map(map)).transpose()?);

    Ok(())
}
