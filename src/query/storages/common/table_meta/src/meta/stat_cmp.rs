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

//! Comparison of persisted min/max statistics.
//!
//! # Why persisted statistics need their own comparison
//!
//! Statistics are written against the schema in effect at the time, but read back against the
//! current one. A metadata-only `ALTER TABLE ... MODIFY COLUMN` that widens a decimal precision
//! leaves already-written block, segment, and snapshot statistics tagged with the previous
//! `DecimalSize`, so a single column can hold a mix of precisions.
//!
//! Comparing two decimals of different `DecimalSize` has no defined ordering:
//! `DecimalScalar::partial_cmp` returns `None`. What makes that dangerous is how the surrounding
//! impls absorb it:
//!
//! - `Ord for Scalar` is `partial_cmp(..).unwrap_or(Ordering::Equal)`, so `<`, `>`, `cmp`,
//!   `min_by`, `max_by`, and `sort_by` all silently report the values as equal.
//! - `PartialEq for Scalar` is `partial_cmp(..) == Some(Equal)`, so `==` silently reports them as
//!   different.
//!
//! Neither path raises an error, so a stale precision quietly degrades pruning and ordering, and
//! `min_by`/`max_by` can even emit a range whose min and max carry different sizes.
//!
//! # What this module does
//!
//! A decimal's raw integer value is unaffected by its precision: only the scale decides where the
//! decimal point sits. Two statistics scalars of the same scale are therefore comparable by their
//! raw values, whatever precision or storage variant they are tagged with. That is exactly the
//! situation a metadata-only widening produces, so these helpers compare by scale and value and
//! ignore precision. Genuinely incomparable inputs (different scale, different type) return
//! `None`, and callers must fall back conservatively rather than assume equality.
//!
//! Because comparability does not depend on the current schema, callers need no type information.

use std::cmp::Ordering;

use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::i256;

/// Compare two scalars taken from persisted statistics.
///
/// Decimals of equal scale compare by raw value regardless of precision or storage variant.
/// Returns `None` when the values are not comparable; callers must treat that conservatively
/// instead of assuming `Ordering::Equal` the way `Ord for Scalar` does.
pub fn try_cmp_stat_scalars(left: &ScalarRef<'_>, right: &ScalarRef<'_>) -> Option<Ordering> {
    match (left, right) {
        (ScalarRef::Decimal(left), ScalarRef::Decimal(right)) => {
            if left.scale() != right.scale() {
                return None;
            }
            // Widening a precision never rewrites the stored value, and it may promote the
            // storage variant, so compare the raw values in the widest representation.
            Some(left.as_decimal::<i256>().cmp(&right.as_decimal::<i256>()))
        }
        // `Decimal` mixed with anything else is genuinely incomparable; `partial_cmp` already
        // reports `None` for that, and for every other type it is well defined.
        _ => left.partial_cmp(right),
    }
}

/// Compare two statistics tuples lexicographically, as used by cluster statistics.
///
/// Returns `None` if the tuples have different lengths or any position is incomparable.
pub fn try_cmp_stat_scalar_slices(left: &[Scalar], right: &[Scalar]) -> Option<Ordering> {
    if left.len() != right.len() {
        return None;
    }
    for (left, right) in left.iter().zip(right) {
        match try_cmp_stat_scalars(&left.as_ref(), &right.as_ref())? {
            Ordering::Equal => {}
            ordering => return Some(ordering),
        }
    }
    Some(Ordering::Equal)
}

/// A total order over statistics scalars, for use as a sort or map key.
///
/// [`try_cmp_stat_scalars`] is the right choice for pruning and range decisions, because it
/// reports when two values cannot be ordered. Collection keys cannot express that: a `BTreeMap`
/// whose comparator returns `Equal` for distinct values silently merges them. This function
/// therefore always yields an ordering.
///
/// Decimals order by scale first, then by raw value, so values differing only in precision compare
/// exactly as [`try_cmp_stat_scalars`] would. Values that are genuinely incomparable get an
/// arbitrary but stable order rather than being treated as equal.
pub fn total_cmp_stat_scalars(left: &ScalarRef<'_>, right: &ScalarRef<'_>) -> Ordering {
    match (left, right) {
        (ScalarRef::Decimal(left), ScalarRef::Decimal(right)) => left
            .scale()
            .cmp(&right.scale())
            .then_with(|| left.as_decimal::<i256>().cmp(&right.as_decimal::<i256>())),
        // `Ord for ScalarRef` is only lossy for decimals of differing size; every other pair it
        // reports as `Equal` is genuinely equal.
        _ => left.cmp(right),
    }
}

/// Lexicographic [`total_cmp_stat_scalars`] over tuples, for use as a map key.
pub fn total_cmp_stat_scalar_slices(left: &[Scalar], right: &[Scalar]) -> Ordering {
    left.iter()
        .map(Scalar::as_ref)
        .zip(right.iter().map(Scalar::as_ref))
        .map(|(left, right)| total_cmp_stat_scalars(&left, &right))
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

/// Whether `[left_min, left_max]` and `[right_min, right_max]` are provably disjoint.
///
/// Returns `None` when that cannot be decided, which callers must read as "may overlap".
pub fn try_stat_ranges_disjoint(
    left_min: &Scalar,
    left_max: &Scalar,
    right_min: &Scalar,
    right_max: &Scalar,
) -> Option<bool> {
    let left_above = try_cmp_stat_scalars(&left_min.as_ref(), &right_max.as_ref())?;
    let right_above = try_cmp_stat_scalars(&right_min.as_ref(), &left_max.as_ref())?;
    Some(left_above == Ordering::Greater || right_above == Ordering::Greater)
}

/// Retag a decimal statistics scalar with `target_size`, keeping the raw value.
///
/// Returns `None` when the persisted value cannot be reinterpreted at `target_size`, which means
/// it did not come from a metadata-only widening of this column. Non-decimal scalars and nulls
/// are returned unchanged.
pub fn retag_stat_scalar(scalar: &Scalar, target_size: DecimalSize) -> Option<Scalar> {
    let Scalar::Decimal(decimal) = scalar else {
        return Some(scalar.clone());
    };
    if decimal.scale() != target_size.scale()
        || decimal.size().precision() > target_size.precision()
    {
        return None;
    }
    if decimal.size() == target_size {
        return Some(scalar.clone());
    }
    // Keep the storage variant of the persisted value: statistics written by an external Parquet
    // reader carry the variant implied by the Parquet physical type, which need not match the
    // variant the current precision would imply.
    Some(Scalar::Decimal(match decimal {
        DecimalScalar::Decimal64(value, _) => DecimalScalar::Decimal64(*value, target_size),
        DecimalScalar::Decimal128(value, _) => DecimalScalar::Decimal128(*value, target_size),
        DecimalScalar::Decimal256(value, _) => DecimalScalar::Decimal256(*value, target_size),
    }))
}

/// The smallest size that can hold every non-null decimal bound in `scalars`.
///
/// Returns `None` if the values are not decimals of a single scale and storage variant, in which
/// case there is no single size to align them to.
pub fn common_stat_decimal_size(scalars: &[&Scalar]) -> Option<DecimalSize> {
    let mut common: Option<DecimalSize> = None;
    for scalar in scalars {
        if scalar.is_null() {
            continue;
        }
        let Scalar::Decimal(decimal) = scalar else {
            return None;
        };
        let size = decimal.size();
        common = match common {
            None => Some(size),
            Some(current) if current.scale() == size.scale() => Some(DecimalSize::new_unchecked(
                current.precision().max(size.precision()),
                current.scale(),
            )),
            Some(_) => return None,
        };
    }
    common
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d64(value: i64, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal64(
            value,
            DecimalSize::new(precision, scale).unwrap(),
        ))
    }

    fn d128(value: i128, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal128(
            value,
            DecimalSize::new(precision, scale).unwrap(),
        ))
    }

    fn cmp(left: &Scalar, right: &Scalar) -> Option<Ordering> {
        try_cmp_stat_scalars(&left.as_ref(), &right.as_ref())
    }

    #[test]
    fn test_raw_ord_collapses_mixed_precision_to_equal() {
        // The hazard this module exists for: `Ord for Scalar` reports two plainly different
        // values as equal, and `PartialEq` reports two equal values as different.
        let stale = d64(100, 10, 2);
        let current = d64(900, 15, 2);
        assert_eq!(stale.cmp(&current), Ordering::Equal);
        assert_ne!(d64(100, 10, 2), d64(100, 15, 2));

        assert_eq!(cmp(&stale, &current), Some(Ordering::Less));
        assert_eq!(cmp(&current, &stale), Some(Ordering::Greater));
        assert_eq!(
            cmp(&d64(100, 10, 2), &d64(100, 15, 2)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_compares_across_storage_variants() {
        // A widening may promote the storage variant, and statistics written by an external
        // Parquet reader carry the variant implied by the Parquet physical type.
        assert_eq!(
            cmp(&d64(100, 10, 2), &d128(900, 25, 2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&d128(900, 25, 2), &d64(100, 10, 2)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            cmp(&d64(500, 10, 2), &d128(500, 25, 2)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_negative_values_compare_by_raw_value() {
        assert_eq!(
            cmp(&d64(-900, 10, 2), &d64(100, 15, 2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&d64(-100, 10, 2), &d64(-900, 15, 2)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn test_incomparable_inputs_report_none() {
        // A different scale means the raw value denotes a different number.
        assert_eq!(cmp(&d64(100, 10, 4), &d64(900, 15, 2)), None);
        // Decimal against a non-decimal is genuinely incomparable.
        assert_eq!(cmp(&d64(100, 10, 2), &Scalar::String("x".into())), None);
    }

    #[test]
    fn test_non_decimal_scalars_are_unaffected() {
        assert_eq!(
            cmp(&Scalar::String("a".into()), &Scalar::String("b".into())),
            Some(Ordering::Less)
        );
        assert_eq!(cmp(&Scalar::Null, &Scalar::Null), Some(Ordering::Equal));
        // Nulls sort above values, matching `Scalar::partial_cmp`.
        assert_eq!(
            cmp(&Scalar::Null, &d64(100, 10, 2)),
            Some(Ordering::Greater)
        );
        assert_eq!(cmp(&d64(100, 10, 2), &Scalar::Null), Some(Ordering::Less));
    }

    #[test]
    fn test_slice_comparison_is_lexicographic() {
        let left = vec![d64(100, 10, 2), d64(200, 10, 2)];
        let right = vec![d64(100, 15, 2), d64(300, 15, 2)];
        assert_eq!(
            try_cmp_stat_scalar_slices(&left, &right),
            Some(Ordering::Less)
        );

        // Length mismatch and any incomparable position report `None`.
        assert_eq!(try_cmp_stat_scalar_slices(&left, &left[..1]), None);
        let bad_scale = vec![d64(100, 10, 4), d64(200, 10, 4)];
        assert_eq!(try_cmp_stat_scalar_slices(&left, &bad_scale), None);
    }

    #[test]
    fn test_range_disjointness() {
        // [1.00, 2.00] stale vs [7.00, 8.00] current: provably disjoint.
        let disjoint = try_stat_ranges_disjoint(
            &d64(100, 10, 2),
            &d64(200, 10, 2),
            &d64(700, 15, 2),
            &d64(800, 15, 2),
        );
        assert_eq!(disjoint, Some(true));

        // [1.00, 9.00] contains [7.00, 8.00].
        let overlapping = try_stat_ranges_disjoint(
            &d64(100, 10, 2),
            &d64(900, 10, 2),
            &d64(700, 15, 2),
            &d64(800, 15, 2),
        );
        assert_eq!(overlapping, Some(false));

        // Touching at a boundary counts as overlapping.
        let touching = try_stat_ranges_disjoint(
            &d64(100, 10, 2),
            &d64(700, 10, 2),
            &d64(700, 15, 2),
            &d64(800, 15, 2),
        );
        assert_eq!(touching, Some(false));

        // Incomparable bounds cannot prove disjointness.
        let unknown = try_stat_ranges_disjoint(
            &d64(100, 10, 4),
            &d64(200, 10, 4),
            &d64(700, 15, 2),
            &d64(800, 15, 2),
        );
        assert_eq!(unknown, None);
    }

    #[test]
    fn test_retag_keeps_value_and_variant() {
        let size = DecimalSize::new(15, 2).unwrap();
        assert_eq!(
            retag_stat_scalar(&d64(100, 10, 2), size),
            Some(d64(100, 15, 2))
        );
        // The persisted storage variant is preserved even when the target precision would imply
        // a narrower one.
        assert_eq!(
            retag_stat_scalar(&d128(100, 10, 2), size),
            Some(d128(100, 15, 2))
        );
        // Nulls and non-decimals pass through.
        assert_eq!(retag_stat_scalar(&Scalar::Null, size), Some(Scalar::Null));
        let text = Scalar::String("x".into());
        assert_eq!(retag_stat_scalar(&text, size), Some(text));

        // Narrowing or a scale change is not a metadata-only widening.
        assert_eq!(retag_stat_scalar(&d64(100, 20, 2), size), None);
        assert_eq!(retag_stat_scalar(&d64(100, 10, 4), size), None);
    }

    #[test]
    fn test_common_decimal_size_takes_widest_precision() {
        let stale = d64(100, 10, 2);
        let current = d64(900, 15, 2);
        assert_eq!(
            common_stat_decimal_size(&[&stale, &current]),
            DecimalSize::new(15, 2).ok()
        );

        // Nulls are skipped; an all-null input has no size to align to.
        assert_eq!(
            common_stat_decimal_size(&[&Scalar::Null, &current]),
            DecimalSize::new(15, 2).ok()
        );
        assert_eq!(common_stat_decimal_size(&[&Scalar::Null]), None);

        // Mixed scales and non-decimals have no common size.
        assert_eq!(common_stat_decimal_size(&[&stale, &d64(100, 10, 4)]), None);
        assert_eq!(
            common_stat_decimal_size(&[&stale, &Scalar::String("x".into())]),
            None
        );
    }
}
