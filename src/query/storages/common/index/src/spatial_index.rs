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

use databend_common_expression::Domain;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use geo::Rect;

use crate::IndexFile;
use crate::IndexMeta;

pub type SpatialIndexMeta = IndexMeta;
pub type SpatialIndexFile = IndexFile;

pub fn rects_distance_intersect(
    block_rect: &Rect<f64>,
    query_rect: &Option<Rect<f64>>,
    distance: f64,
) -> bool {
    if let Some(query_rect) = query_rect {
        block_rect.min().x <= query_rect.max().x + distance
            && block_rect.max().x >= query_rect.min().x - distance
            && block_rect.min().y <= query_rect.max().y + distance
            && block_rect.max().y >= query_rect.min().y - distance
    } else {
        false
    }
}

pub fn rects_intersect(block_rect: &Rect<f64>, query_rect: &Option<Rect<f64>>) -> bool {
    if let Some(query_rect) = query_rect {
        block_rect.min().x <= query_rect.max().x
            && block_rect.max().x >= query_rect.min().x
            && block_rect.min().y <= query_rect.max().y
            && block_rect.max().y >= query_rect.min().y
    } else {
        false
    }
}

pub fn rect_contains(block_rect: &Rect<f64>, query_rect: &Option<Rect<f64>>) -> bool {
    if let Some(query_rect) = query_rect {
        block_rect.min().x <= query_rect.min().x
            && block_rect.min().y <= query_rect.min().y
            && block_rect.max().x >= query_rect.max().x
            && block_rect.max().y >= query_rect.max().y
    } else {
        false
    }
}

pub fn spatial_false_domain(return_type: &DataType, has_null: bool) -> Domain {
    let bool_domain = Domain::Boolean(BooleanDomain {
        has_false: true,
        has_true: false,
    });
    if return_type.is_nullable() {
        Domain::Nullable(NullableDomain {
            has_null,
            value: Some(Box::new(bool_domain)),
        })
    } else {
        bool_domain
    }
}

// Convert a scalar distance into a conservative f64 upper bound.
//
// Decimal values and large integers may lose precision when converted to f64,
// so those cases are rounded upward to the next representable f64.
pub fn scalar_to_distance_threshold(scalar: &Scalar) -> Option<f64> {
    let (threshold, needs_upper_bound) = match scalar {
        Scalar::Number(number) => match number {
            NumberScalar::Int8(v) => (*v as f64, false),
            NumberScalar::Int16(v) => (*v as f64, false),
            NumberScalar::Int32(v) => (*v as f64, false),
            NumberScalar::Int64(v) => (*v as f64, *v > (1_i64 << 53)),
            NumberScalar::UInt8(v) => (*v as f64, false),
            NumberScalar::UInt16(v) => (*v as f64, false),
            NumberScalar::UInt32(v) => (*v as f64, false),
            NumberScalar::UInt64(v) => (*v as f64, *v > (1_u64 << 53)),
            NumberScalar::Float32(v) => (v.0 as f64, false),
            NumberScalar::Float64(v) => (v.0, false),
        },
        Scalar::Decimal(decimal) => (
            match decimal {
                DecimalScalar::Decimal64(_, _)
                | DecimalScalar::Decimal128(_, _)
                | DecimalScalar::Decimal256(_, _) => decimal.to_float64(),
            },
            true,
        ),
        _ => return None,
    };

    if !threshold.is_finite() || threshold < 0.0 {
        return None;
    }

    let threshold = if needs_upper_bound && threshold != f64::MAX {
        threshold.next_up()
    } else {
        threshold
    };
    Some(threshold)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;

    use crate::scalar_to_distance_threshold;

    #[test]
    fn test_scalar_to_distance_threshold_uses_conservative_upper_bound() {
        let threshold = scalar_to_distance_threshold(&Scalar::Number(NumberScalar::UInt64(
            9_007_199_254_740_993,
        )))
        .unwrap();
        assert_eq!(threshold, 9_007_199_254_740_994.0);
    }

    #[test]
    fn test_scalar_to_distance_threshold_keeps_exact_integer() {
        let threshold =
            scalar_to_distance_threshold(&Scalar::Number(NumberScalar::UInt64(100))).unwrap();
        assert_eq!(threshold, 100.0);
    }
}
