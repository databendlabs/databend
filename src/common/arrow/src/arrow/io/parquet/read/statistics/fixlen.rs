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

use ethnum::I256;
use parquet2::statistics::FixedLenStatistics;
use parquet2::statistics::Statistics as ParquetStatistics;

use super::super::convert_days_ms;
use super::super::convert_i128;
use crate::arrow::array::*;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::convert_i256;
use crate::arrow::types::days_ms;
use crate::arrow::types::i256;

pub(super) fn push_i128(
    from: Option<&dyn ParquetStatistics>,
    n: usize,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i128>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i128>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());

    min.push(from.and_then(|s| s.min_value.as_deref().map(|x| convert_i128(x, n))));
    max.push(from.and_then(|s| s.max_value.as_deref().map(|x| convert_i128(x, n))));

    Ok(())
}

pub(super) fn push_i256_with_i128(
    from: Option<&dyn ParquetStatistics>,
    n: usize,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i256>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i256>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());

    min.push(from.and_then(|s| {
        s.min_value
            .as_deref()
            .map(|x| i256(I256::new(convert_i128(x, n))))
    }));
    max.push(from.and_then(|s| {
        s.max_value
            .as_deref()
            .map(|x| i256(I256::new(convert_i128(x, n))))
    }));

    Ok(())
}

pub(super) fn push_i256(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i256>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i256>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());

    min.push(from.and_then(|s| s.min_value.as_deref().map(convert_i256)));
    max.push(from.and_then(|s| s.max_value.as_deref().map(convert_i256)));

    Ok(())
}

pub(super) fn push(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutableFixedSizeBinaryArray>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutableFixedSizeBinaryArray>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());
    min.push(from.and_then(|s| s.min_value.as_ref()));
    max.push(from.and_then(|s| s.max_value.as_ref()));
    Ok(())
}

fn convert_year_month(value: &[u8]) -> i32 {
    i32::from_le_bytes(value[..4].try_into().unwrap())
}

pub(super) fn push_year_month(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i32>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<i32>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());

    min.push(from.and_then(|s| s.min_value.as_deref().map(convert_year_month)));
    max.push(from.and_then(|s| s.max_value.as_deref().map(convert_year_month)));

    Ok(())
}

pub(super) fn push_days_ms(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<days_ms>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutablePrimitiveArray<days_ms>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<FixedLenStatistics>().unwrap());

    min.push(from.and_then(|s| s.min_value.as_deref().map(convert_days_ms)));
    max.push(from.and_then(|s| s.max_value.as_deref().map(convert_days_ms)));

    Ok(())
}
