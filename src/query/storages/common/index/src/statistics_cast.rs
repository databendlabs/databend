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

use databend_common_expression::Scalar;
use databend_common_expression::cast_scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::F32;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::ColumnStatistics;

/// Convert physical column statistics into a conservative interval in the
/// query-visible type.
///
/// Range and TopN pruning only require an interval containing every value after
/// the query cast; the conversion does not have to be lossless or injective.
/// This function therefore accepts a restricted set of monotonic numeric casts,
/// strictly casts both endpoints, and expands floating-point bounds by one ULP.
pub fn cast_virtual_column_statistics(
    statistics: &ColumnStatistics,
    physical_type: &DataType,
    requested_type: &DataType,
) -> Option<ColumnStatistics> {
    if physical_type == requested_type {
        return Some(statistics.clone());
    }

    let physical_type = physical_type.remove_nullable();
    let requested_type = requested_type.remove_nullable();
    if !is_monotonic_statistics_cast(&physical_type, &requested_type) {
        return None;
    }

    let statistics_type = statistics
        .min
        .as_ref()
        .infer_common_type(&statistics.max.as_ref())?;
    if statistics_type.remove_nullable() != physical_type {
        return None;
    }

    let min = cast_scalar(
        None,
        statistics.min.clone(),
        &requested_type,
        &BUILTIN_FUNCTIONS,
    )
    .ok()?;
    let max = cast_scalar(
        None,
        statistics.max.clone(),
        &requested_type,
        &BUILTIN_FUNCTIONS,
    )
    .ok()?;
    // Reject malformed metadata or an unexpectedly non-monotonic conversion.
    if min.is_null() || max.is_null() || min > max {
        return None;
    }

    let (min, max) = expand_float_statistics_bounds(min, max)?;
    Some(ColumnStatistics::new(
        min,
        max,
        statistics.null_count,
        statistics.in_memory_size,
        None,
    ))
}

fn is_monotonic_statistics_cast(physical_type: &DataType, requested_type: &DataType) -> bool {
    match (physical_type, requested_type) {
        (DataType::Number(src), DataType::Number(dest)) => {
            src.is_integer()
                || (*src == NumberDataType::Float32 && *dest == NumberDataType::Float64)
        }
        (DataType::Number(src), DataType::Decimal(_)) => src.is_integer(),
        (DataType::Decimal(_), DataType::Decimal(_)) => true,
        (DataType::Decimal(_), DataType::Number(dest)) => dest.is_float(),
        _ => false,
    }
}

fn expand_float_statistics_bounds(min: Scalar, max: Scalar) -> Option<(Scalar, Scalar)> {
    match (min, max) {
        (
            Scalar::Number(NumberScalar::Float32(min)),
            Scalar::Number(NumberScalar::Float32(max)),
        ) if min.is_finite() && max.is_finite() => Some((
            Scalar::Number(NumberScalar::Float32(F32::from(min.next_down()))),
            Scalar::Number(NumberScalar::Float32(F32::from(max.next_up()))),
        )),
        (
            Scalar::Number(NumberScalar::Float64(min)),
            Scalar::Number(NumberScalar::Float64(max)),
        ) if min.is_finite() && max.is_finite() => Some((
            Scalar::Number(NumberScalar::Float64(F64::from(min.next_down()))),
            Scalar::Number(NumberScalar::Float64(F64::from(max.next_up()))),
        )),
        (Scalar::Number(NumberScalar::Float32(_)), Scalar::Number(NumberScalar::Float32(_)))
        | (Scalar::Number(NumberScalar::Float64(_)), Scalar::Number(NumberScalar::Float64(_))) => {
            None
        }
        (min, max) => Some((min, max)),
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::number::F64;

    use super::*;

    #[test]
    fn test_cast_virtual_column_statistics() {
        let uint64_type = DataType::Number(NumberDataType::UInt64);
        let int64_type = DataType::Number(NumberDataType::Int64);
        let statistics = ColumnStatistics::new(
            Scalar::Number(NumberScalar::UInt64(10)),
            Scalar::Number(NumberScalar::UInt64(20)),
            1,
            16,
            Some(11),
        );
        let converted =
            cast_virtual_column_statistics(&statistics, &uint64_type, &int64_type).unwrap();
        assert_eq!(converted.min(), &Scalar::Number(NumberScalar::Int64(10)));
        assert_eq!(converted.max(), &Scalar::Number(NumberScalar::Int64(20)));
        assert_eq!(converted.null_count, 1);
        assert_eq!(converted.in_memory_size, 16);
        assert_eq!(converted.distinct_of_values, None);

        let overflowing = ColumnStatistics::new(
            Scalar::Number(NumberScalar::UInt64(i64::MAX as u64)),
            Scalar::Number(NumberScalar::UInt64(i64::MAX as u64 + 1)),
            0,
            16,
            None,
        );
        assert!(cast_virtual_column_statistics(&overflowing, &uint64_type, &int64_type).is_none());

        let decimal_size = DecimalSize::new(18, 1).unwrap();
        let decimal_type = DataType::Decimal(decimal_size);
        let float64_type = DataType::Number(NumberDataType::Float64);
        let decimal_statistics = ColumnStatistics::new(
            Scalar::Decimal(DecimalScalar::Decimal64(955, decimal_size)),
            Scalar::Decimal(DecimalScalar::Decimal64(1005, decimal_size)),
            0,
            16,
            None,
        );
        let converted =
            cast_virtual_column_statistics(&decimal_statistics, &decimal_type, &float64_type)
                .unwrap();
        assert_eq!(
            converted.min(),
            &Scalar::Number(NumberScalar::Float64(F64::from(95.5_f64.next_down())))
        );
        assert_eq!(
            converted.max(),
            &Scalar::Number(NumberScalar::Float64(F64::from(100.5_f64.next_up())))
        );
    }
}
