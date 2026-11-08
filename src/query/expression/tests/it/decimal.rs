// Copyright 2023 Datafuse Labs.
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
use databend_common_expression::function_stat::DomainStatBounds;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDomain;
use databend_common_expression::types::F64;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalDataKind;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_statistics::StatBounds;
use num_bigint::BigInt;
use pretty_assertions::assert_eq;

// SQL tests cover decimal values and import failures. Keep direct tests for the
// parser contracts that SQL imports do not expose: truncation and prefix length.
#[test]
fn test_decimal_with_size_modes() {
    fn check<T: Decimal>() {
        let size = DecimalSize::new_unchecked(6, 3);
        for (text, truncated, rounded) in [
            ("+0001.2345", 1234, 1235),
            ("-0001.2345", -1234, -1235),
            (".0005", 0, 1),
            ("-.0005", 0, -1),
            ("0.", 0, 0),
            ("12.345E-1", 1234, 1235),
            (".0210e3", 21000, 21000),
            ("0120.0210e-1", 12002, 12002),
            ("1.5e+1", 15000, 15000),
            // Preserve the historical zero-exponent interpretation and consume e/sign.
            ("1e", 1000, 1000),
            ("1e+", 1000, 1000),
            ("1e-", 1000, 1000),
            ("1.5e", 1500, 1500),
            ("1.5E+", 1500, 1500),
            ("-1.5e-", -1500, -1500),
            ("0e+", 0, 0),
            ("1e-2147483648", 0, 0),
            ("0e2147483647", 0, 0),
        ] {
            for rounding_mode in [false, true] {
                let expected = T::from_i64(if rounding_mode { rounded } else { truncated });
                // A later field's exponent must not affect the current number.
                for suffix in ["", ",2e10", ")", "\t3", "\n4"] {
                    let input = format!("{text}{suffix}");
                    assert_eq!(
                        read_decimal_with_size::<T>(input.as_bytes(), size, false, rounding_mode)
                            .unwrap(),
                        (expected, text.len()),
                        "{input:?}, rounding={rounding_mode}"
                    );
                    let exact =
                        read_decimal_with_size::<T>(input.as_bytes(), size, true, rounding_mode);
                    if suffix.is_empty() {
                        assert_eq!(exact.unwrap(), (expected, text.len()));
                    } else {
                        assert!(exact.is_err(), "{input:?}");
                    }
                }
            }
        }
        // Truncation succeeds at the precision limit where rounding would overflow.
        for (text, coefficient) in [("999.9995", 999999), ("-999.9995", -999999)] {
            assert_eq!(
                read_decimal_with_size::<T>(text.as_bytes(), size, true, false).unwrap(),
                (T::from_i64(coefficient), text.len())
            );
            assert!(read_decimal_with_size::<T>(text.as_bytes(), size, true, true).is_err());
        }
        for text in ["1e10,", "1e-2147483649,", "0e9999999999,"] {
            assert!(
                read_decimal_with_size::<T>(text.as_bytes(), size, false, true).is_err(),
                "{text}"
            );
        }
        // Consume exactly the coefficient across full/partial 18-digit chunks,
        // including all-zero chunks, without absorbing the next field.
        let size = DecimalSize::new_unchecked(T::MAX_PRECISION, 0);
        for digits in [17, 18, 19, 35, 36, 37, 53, 54, 55, 71, 72, 73] {
            if digits > T::MAX_PRECISION as usize {
                continue;
            }
            for sign in ["", "-"] {
                let text = format!("{sign}1{}1", "0".repeat(digits - 2));
                let expected = T::from_bigint(text.parse::<BigInt>().unwrap()).unwrap();
                let input = format!("{text},2e10");
                assert_eq!(
                    read_decimal_with_size::<T>(input.as_bytes(), size, false, true).unwrap(),
                    (expected, text.len()),
                    "{input}"
                );
            }
        }
    }
    check::<i64>();
    check::<i128>();
    check::<i256>();
}

#[test]
fn test_decimal_with_size_invalid_text() {
    let size = DecimalSize::new_unchecked(18, 4);
    for text in ["", ".", "+", "-", ".e1", "a"] {
        for exact in [false, true] {
            for rounding_mode in [false, true] {
                assert!(
                    read_decimal_with_size::<i64>(text.as_bytes(), size, exact, rounding_mode)
                        .is_err(),
                    "{text}, exact={exact}, rounding={rounding_mode}"
                );
            }
        }
    }
    // Exact mode validates the suffix even when the result would round to zero.
    for text in ["0x", "1e-100x", "1.2.3", "1e2e3", "1ee2", "1e+x", "1e--1"] {
        assert!(
            read_decimal_with_size::<i64>(text.as_bytes(), size, true, true).is_err(),
            "{text}"
        );
    }
}

#[test]
fn test_decimal_common_type() {
    let cases = vec![
        ((3, 3), DataType::Number(NumberDataType::UInt8), (6, 3)),
        ((3, 2), DataType::Number(NumberDataType::UInt8), (5, 2)),
        ((6, 3), DataType::Number(NumberDataType::UInt8), (6, 3)),
    ];

    for (a, b, c) in cases {
        let l = DataType::Decimal(DecimalSize::new_unchecked(a.0, a.1));
        let expected = DataType::Decimal(DecimalSize::new_unchecked(c.0, c.1));
        let r = common_super_type(l, b, &[]);
        assert_eq!(r, Some(expected));
    }
}

/// Checks `T::from_float` against `expected` in every storage that can hold `size`.
fn assert_from_float(value: f64, size: DecimalSize, rounding: bool, expected: Option<&BigInt>) {
    fn check<T: Decimal>(value: f64, size: DecimalSize, rounding: bool, expected: Option<&BigInt>) {
        assert_eq!(
            T::from_float(value, size, rounding),
            expected.map(|coefficient| T::from_bigint(coefficient.clone()).unwrap()),
            "{value:?} -> {size:?}, storage={}, rounding={rounding}",
            std::any::type_name::<T>(),
        );
    }
    if size.precision() <= i64::MAX_PRECISION {
        check::<i64>(value, size, rounding, expected);
    }
    if size.precision() <= i128::MAX_PRECISION {
        check::<i128>(value, size, rounding, expected);
    }
    check::<i256>(value, size, rounding, expected);
}

// Literal results, independent of the oracle below. Computing `value * 10^scale`
// in f64 rounded away the last digits of these (#20781).
#[test]
fn test_float_to_decimal_examples() {
    for (value, (precision, scale), truncated, rounded) in [
        (
            415210.34,
            (38, 10),
            Some(4152103400000000_i128),
            Some(4152103400000000),
        ),
        (
            284957.46,
            (38, 12),
            Some(284957460000000020),
            Some(284957460000000021),
        ),
        (
            1.1,
            (38, 18),
            Some(1100000000000000088),
            Some(1100000000000000089),
        ),
        // Rounding happens once, after the exact product; rounding earlier,
        // even with guard bits, changes the last digit.
        (
            0.1,
            (17, 17),
            Some(10000000000000000),
            Some(10000000000000001),
        ),
        (
            0.3,
            (18, 18),
            Some(299999999999999988),
            Some(299999999999999989),
        ),
        (
            1e30,
            (76, 0),
            Some(1000000000000000019884624838656),
            Some(1000000000000000019884624838656),
        ),
        (123.456, (38, 0), Some(123), Some(123)),
        (0.125, (18, 2), Some(12), Some(13)),
        (f64::from_bits(1), (76, 76), Some(0), Some(0)),
        // Overflow is checked after rounding.
        (9.5, (1, 0), Some(9), None),
        (f64::NAN, (38, 10), None, None),
        (f64::INFINITY, (76, 0), None, None),
    ] {
        let size = DecimalSize::new(precision, scale).unwrap();
        for (rounding, expected) in [(false, truncated), (true, rounded)] {
            let expected = expected.map(BigInt::from);
            assert_from_float(value, size, rounding, expected.as_ref());
            let negated = expected.map(|coefficient| -coefficient);
            assert_from_float(-value, size, rounding, negated.as_ref());
        }
    }
}

/// Exact `value * 10^scale`, truncated or rounded half away from zero; `None`
/// if `value` is not finite or the result exceeds the precision. Computed as
/// `mantissa * 10^scale / 2^-exponent` in BigInt, independently of the
/// production `5^scale` products and binary shifts.
fn float_coefficient_oracle(value: f64, size: DecimalSize, rounding: bool) -> Option<BigInt> {
    if !value.is_finite() {
        return None;
    }
    let (mantissa, exponent, sign) = num_traits::Float::integer_decode(value);
    let numerator = BigInt::from(mantissa) * sign * BigInt::from(10).pow(size.scale() as u32);
    let coefficient = if exponent >= 0 {
        numerator << exponent as usize
    } else {
        let divisor = BigInt::from(1) << (-exponent) as usize;
        let quotient = &numerator / &divisor;
        let remainder = &numerator % &divisor;
        if rounding && num_traits::Signed::abs(&remainder) * 2 >= divisor {
            quotient + sign
        } else {
            quotient
        }
    };
    let limit = BigInt::from(10).pow(size.precision() as u32);
    (num_traits::Signed::abs(&coefficient) < limit).then_some(coefficient)
}

/// Checks `value` and `-value` against the oracle in both modes.
fn check_from_float(value: f64, size: DecimalSize) {
    for value in [value, -value] {
        for rounding in [false, true] {
            let expected = float_coefficient_oracle(value, size, rounding);
            assert_from_float(value, size, rounding, expected.as_ref());
        }
    }
}

#[test]
fn test_float_to_decimal_matches_oracle_on_edges() {
    let values = [
        415210.34,
        363955.09,
        135968.86,
        284957.46,
        1.1,
        0.1,
        0.3,
        0.0,
        0.5,
        1.5,
        2.5,
        9.5,
        99.5,
        0.125,
        1.0,
        f64::from_bits(0.5_f64.to_bits() - 1),
        f64::from_bits(0.5_f64.to_bits() + 1),
        f64::from_bits(2.0_f64.to_bits() - 1),
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
        1e18,
        1e38,
        1e76,
        1e-38,
        1e-76,
        9007199254740991.0,
        9007199254740992.0,
        f32::from_bits(1) as f64,
        f32::MIN_POSITIVE as f64,
        f32::MAX as f64,
        1.1_f32 as f64,
        // At scale 33 these subnormal significands straddle the u128 product limit.
        f64::from_bits(1_u64 << 51),
        f64::from_bits((1_u64 << 52) - 1),
        // Native u128 right shift by exactly 128, with a nonzero rounding bit.
        f64::from_bits(((1023_u64 - 108) << 52) | ((1_u64 << 52) - 1)),
    ];
    // Powers of two normalize to a one-bit significand and stay on the native
    // path at every scale; long significands such as 0.1 take the wide fallback
    // at high scales.
    for precision in 1..=76 {
        for scale in 0..=precision {
            let size = DecimalSize::new(precision, scale).unwrap();
            for value in values {
                check_from_float(value, size);
            }
        }
    }
}

#[test]
fn test_float_to_decimal_native_product_boundaries() {
    for (bits, precision, max_product) in [(64, 18, u64::MAX as u128), (128, 38, u128::MAX)] {
        for scale in 0..=precision {
            let factor = 5_u128.pow(scale as u32);
            let limit = max_product / factor;
            if limit >= (1_u128 << 53) - 1 {
                // Every f64 significand fits; there is no fallback boundary.
                continue;
            }
            // Use odd mantissas so normalization cannot move the boundary.
            let below = (limit as u64 - 1) | 1;
            assert!(below as u128 <= limit);
            assert!(below as u128 + 2 > limit);
            let size = DecimalSize::new(precision, scale).unwrap();
            for mantissa in [below - 2, below, below + 2] {
                for shift in [-bits - 1, -bits, -bits + 1, -1, 0, 1] {
                    // Both factors are exact, and all generated values are normal.
                    let value = mantissa as f64 * 2_f64.powi(shift - scale as i32);
                    check_from_float(value, size);
                }
            }
        }
    }
}

proptest::proptest! {
    // Odd significands with shifts around the coefficient widths, at scales on
    // both sides of the native product limits.
    #[test]
    fn test_float_to_decimal_native_and_fallback_matches_oracle(
        mantissa in 1_u64..(1_u64 << 53),
        shift in -256_i32..=10,
        scale in proptest::sample::select(vec![0_u8, 1, 4, 5, 18, 31, 32, 33, 38, 60, 76]),
    ) {
        let precision = if scale <= 18 { 18 } else if scale <= 38 { 38 } else { 76 };
        let value = (mantissa | 1) as f64 * 2_f64.powi(shift - scale as i32);
        check_from_float(value, DecimalSize::new(precision, scale).unwrap());
    }

    #[test]
    fn test_float_to_decimal_matches_oracle(
        bits: u64,
        float32_bits: u32,
        precision in 1_u8..=76,
        scale in 0_u8..=76,
    ) {
        let size = DecimalSize::new(precision, scale.min(precision)).unwrap();
        check_from_float(f64::from_bits(bits), size);
        check_from_float(f32::from_bits(float32_bits) as f64, size);
    }
}

// Decimal stats and constants reach `StatBounds::Float` through `to_float64`
// and come back as a decimal domain. That domain must still contain the
// original value.
fn decimal_stat_round_trip<T: Decimal>(min: T, max: T, size: DecimalSize) -> (T, T) {
    let domain = T::upcast_domain(SimpleDomain { min, max }, size);
    let DomainStatBounds::Bounds(bounds) = domain.stat_bounds() else {
        panic!("expected finite bounds for {domain:?}");
    };
    let domain = Domain::from_bounds(&DataType::Decimal(size), bounds, false).unwrap();
    let range = T::try_downcast_domain(domain.as_decimal().unwrap()).unwrap();
    (range.min, range.max)
}

/// Checks the singleton domains of `coefficient` and `-coefficient`. Up to 15
/// digits an f64 is fine enough to recover the value exactly.
fn check_decimal_stat_round_trip(coefficient: &BigInt, size: DecimalSize) {
    fn check<T: Decimal>(coefficient: &BigInt, size: DecimalSize) {
        let value = T::from_bigint(coefficient.clone()).unwrap();
        let (min, max) = decimal_stat_round_trip(value, value, size);
        assert!(
            min <= value && value <= max,
            "{value} at {size:?} became [{min}, {max}]"
        );
        if size.precision() <= 15 {
            assert_eq!((min, max), (value, value), "{value} at {size:?}");
        }
    }
    for coefficient in [coefficient.clone(), -coefficient] {
        match size.data_kind() {
            DecimalDataKind::Decimal64 => check::<i64>(&coefficient, size),
            DecimalDataKind::Decimal128 => check::<i128>(&coefficient, size),
            DecimalDataKind::Decimal256 => check::<i256>(&coefficient, size),
        }
    }
}

#[test]
fn test_decimal_stat_bounds_contain_original_value() {
    // Truncating the nearest float loses these: 0.3 -> 0.2999... -> 0.2.
    let size = DecimalSize::new(1, 1).unwrap();
    assert_eq!(decimal_stat_round_trip(3_i64, 3, size), (3, 3));
    assert_eq!(decimal_stat_round_trip(-7_i64, -7, size), (-7, -7));
    assert_eq!(decimal_stat_round_trip(-7_i64, 3, size), (-7, 3));

    // Precisions on both sides of the 15-digit exact limit and the storages.
    for precision in [1, 2, 15, 16, 17, 18, 19, 30, 38, 39, 50, 76] {
        let limit = BigInt::from(10).pow(precision as u32);
        let digits = "1234567890".repeat(8)[..precision as usize]
            .parse::<BigInt>()
            .unwrap();
        let mut coefficients = vec![
            BigInt::from(0),
            BigInt::from(1),
            BigInt::from(3),
            BigInt::from(7),
            &limit - 1,
            &limit - 2,
            &limit / 10 * 3,
            &limit / 10 * 7,
            &limit / 10 + 1,
            digits,
        ];
        coefficients.retain(|coefficient| coefficient < &limit);
        for scale in [0, precision / 2, precision] {
            let size = DecimalSize::new(precision, scale).unwrap();
            for coefficient in &coefficients {
                check_decimal_stat_round_trip(coefficient, size);
            }
        }
    }
}

#[test]
fn test_decimal_stat_bounds_from_non_finite_float() {
    let size = DecimalSize::new(5, 2).unwrap();
    let from_bounds = |min: f64, max: f64| {
        Domain::from_bounds(
            &DataType::Decimal(size),
            StatBounds::Float {
                min: F64::from(min),
                max: F64::from(max),
            },
            false,
        )
    };
    // Bounds beyond the precision clamp to the full range; NaN is rejected.
    let full = Domain::Decimal(DecimalDomain::Decimal64(
        SimpleDomain {
            min: -99999,
            max: 99999,
        },
        size,
    ));
    assert_eq!(
        from_bounds(f64::NEG_INFINITY, f64::INFINITY),
        Ok(full.clone())
    );
    assert_eq!(from_bounds(-f64::MAX, f64::MAX), Ok(full));
    assert!(from_bounds(f64::NAN, 1.0).is_err());
    assert!(from_bounds(1.0, f64::NAN).is_err());
}

proptest::proptest! {
    #[test]
    fn test_decimal_stat_bounds_contain_random_value(
        bits: u128,
        precision in 1_u8..=38,
        scale in 0_u8..=38,
    ) {
        let size = DecimalSize::new(precision, scale.min(precision)).unwrap();
        let coefficient = BigInt::from(bits) % BigInt::from(10).pow(precision as u32);
        check_decimal_stat_round_trip(&coefficient, size);
    }
}

#[test]
fn test_from_bigint() {
    let min = i256::from_words(i128::MIN, 0);
    let max = i256::from_words(i128::MAX, -1);
    let boundary = BigInt::from(1) << 255_u32;
    assert_eq!(i256::from_bigint(-boundary.clone()), Some(min));
    assert_eq!(
        i256::from_bigint(-boundary.clone() + 1),
        Some(min + i256::ONE)
    );
    assert_eq!(i256::from_bigint(-boundary.clone() - 1), None);
    assert_eq!(i256::from_bigint(boundary.clone() - 1), Some(max));
    assert_eq!(i256::from_bigint(boundary), None);

    let cases = vec![
        ("0", 0i128),
        ("12345", 12345i128),
        ("-1", -1i128),
        ("-170141183460469231731687303715884105728", i128::MIN),
        ("170141183460469231731687303715884105727", i128::MAX),
    ];

    for (a, b) in cases {
        let r = BigInt::parse_bytes(a.as_bytes(), 10).unwrap();
        assert_eq!(i128::from_bigint(r), Some(b));
    }

    let cases = vec![
        ("0".to_string(), i256::ZERO),
        ("12345".to_string(), i256::from(12345)),
        ("-1".to_string(), i256::from(-1)),
        (
            "12".repeat(25),
            i256::from_str_radix(&"12".repeat(25), 10).unwrap(),
        ),
        (
            "1".repeat(26),
            i256::from_str_radix(&"1".repeat(26), 10).unwrap(),
        ),
        (i256::DECIMAL_MIN.to_string(), i256::DECIMAL_MIN),
        (i256::DECIMAL_MAX.to_string(), i256::DECIMAL_MAX),
    ];

    for (a, b) in cases {
        let r = BigInt::parse_bytes(a.as_bytes(), 10).unwrap();
        assert_eq!(i256::from_bigint(r), Some(b));
    }

    let cases = vec![
        ("1".repeat(78), None),
        ("12".repeat(78), None),
        ("234".repeat(78), None),
    ];

    for (a, b) in cases {
        let r = BigInt::parse_bytes(a.as_bytes(), 10).unwrap();
        assert_eq!(i256::from_bigint(r), b);
    }
}

/// The nearest float to `mantissa * 10^-scale`, from the correctly rounded std parser.
fn nearest<T: std::str::FromStr>(mantissa: impl std::fmt::Display, scale: u8) -> T
where T::Err: std::fmt::Debug {
    format!("{mantissa}e-{scale}").parse().unwrap()
}

#[test]
fn test_decimal_to_float_is_correctly_rounded() {
    // Mantissas beyond 2^53 (2^24 for f32) or scales beyond 10^22 (10^10 for f32) are
    // rounded more than once by a naive integer-cast-then-divide conversion.
    let cases_f64 = [
        (37349299999999996_i128, 17, 0.37349299999999996_f64),
        (15990461000457081, 16, 1.5990461000457081),
        (9553820000000001, 13, 955.3820000000001),
        (-5294551535166176, 10, -529455.1535166176),
        (15, 17, 1.5e-16),
        (1, 38, 1e-38),
        (i128::MAX, 0, i128::MAX as f64),
    ];
    for (mantissa, scale, expected) in cases_f64 {
        assert_eq!(mantissa.to_float64(scale), expected, "{mantissa}e-{scale}");
        assert_eq!(i256::from(mantissa).to_float64(scale), expected);
        if let Ok(m) = i64::try_from(mantissa) {
            assert_eq!(m.to_float64(scale), expected, "{mantissa}e-{scale}");
        }
    }

    let cases_f32 = [
        (33333334_i128, 8, 0.33333334_f32),
        (1, 20, 1e-20),
        (30000004, 8, 0.30000004),
        (15, 17, 1.5e-16),
        // Subnormal f32 results.
        (1, 38, 1e-38),
        (-11, 39, -1.1e-38),
        (i128::MAX, 38, 1.7014118),
    ];
    for (mantissa, scale, expected) in cases_f32 {
        assert_eq!(mantissa.to_float32(scale), expected, "{mantissa}e-{scale}");
        assert_eq!(i256::from(mantissa).to_float32(scale), expected);
        if let Ok(m) = i64::try_from(mantissa) {
            assert_eq!(m.to_float32(scale), expected, "{mantissa}e-{scale}");
        }
    }

    // Beyond the i128 range.
    let big = i256::from_words(i128::MAX / 3, 12345);
    assert_eq!(big.to_float64(76), nearest::<f64>(big, 76));
    assert_eq!(big.to_float32(76), nearest::<f32>(big, 76));
    assert_eq!(
        i256::DECIMAL_MIN.to_float64(0),
        nearest::<f64>(i256::DECIMAL_MIN, 0)
    );
}

#[test]
fn test_decimal_to_float_rounds_halfway_cases() {
    // Mantissas at scale 37 just below, at and just above the midpoint between two
    // adjacent floats `lo` (even significand) and `lo + 1 ulp`. Random inputs almost
    // never hit a midpoint, where a lost remainder flips the result.
    let f64_cases = [
        (
            0x3ff0_0000_0000_0000_u64,
            10000000000000001110223024625156540423_i128,
        ),
        (
            0x3ff0_0000_0000_6072,
            10000000000054823923179014855122659355,
        ),
    ];
    for (lo_bits, below) in f64_cases {
        let (lo, hi) = (f64::from_bits(lo_bits), f64::from_bits(lo_bits + 1));
        assert_eq!(below.to_float64(37), lo, "{below}e-37");
        assert_eq!((below + 1).to_float64(37), hi, "{}e-37", below + 1);
        assert_eq!(i256::from(below + 1).to_float64(37), hi);
    }

    // For f32 the midpoints are exact decimals: a tie rounds to the even `lo`.
    let f32_cases = [
        (0x3f80_0000_u32, 10000000596046447753906250000000000000_i128),
        (0x3f80_6072, 10029433369636535644531250000000000000),
    ];
    for (lo_bits, tie) in f32_cases {
        let (lo, hi) = (f32::from_bits(lo_bits), f32::from_bits(lo_bits + 1));
        assert_eq!((tie - 1).to_float32(37), lo, "{}e-37", tie - 1);
        assert_eq!(tie.to_float32(37), lo, "{tie}e-37");
        assert_eq!((tie + 1).to_float32(37), hi, "{}e-37", tie + 1);
        assert_eq!(i256::from(tie + 1).to_float32(37), hi);
    }
}

proptest::proptest! {
    #[test]
    fn test_decimal128_to_float_matches_nearest(mantissa: i128, scale in 0u8..=38) {
        proptest::prop_assert_eq!(mantissa.to_float64(scale), nearest::<f64>(mantissa, scale));
        proptest::prop_assert_eq!(mantissa.to_float32(scale), nearest::<f32>(mantissa, scale));
    }

    // Uniform i128 values are almost all 127 bits wide; also cover narrow mantissas,
    // which take the fast path or produce small and subnormal results.
    #[test]
    fn test_narrow_decimal128_to_float_matches_nearest(
        mantissa: i128,
        shift in 0u32..127,
        scale in 0u8..=38,
    ) {
        let mantissa = mantissa >> shift;
        proptest::prop_assert_eq!(mantissa.to_float64(scale), nearest::<f64>(mantissa, scale));
        proptest::prop_assert_eq!(mantissa.to_float32(scale), nearest::<f32>(mantissa, scale));
    }

    // Decimal256 values within i128 but beyond scale 38.
    #[test]
    fn test_decimal256_small_mantissa_to_float_matches_nearest(
        mantissa: i128,
        shift in 0u32..127,
        scale in 39u8..=76,
    ) {
        let mantissa = i256::from(mantissa >> shift);
        proptest::prop_assert_eq!(mantissa.to_float64(scale), nearest::<f64>(mantissa, scale));
        proptest::prop_assert_eq!(mantissa.to_float32(scale), nearest::<f32>(mantissa, scale));
    }

    #[test]
    fn test_decimal64_to_float_matches_nearest(mantissa: i64, scale in 0u8..=18) {
        proptest::prop_assert_eq!(mantissa.to_float64(scale), nearest::<f64>(mantissa, scale));
        proptest::prop_assert_eq!(mantissa.to_float32(scale), nearest::<f32>(mantissa, scale));
    }

    #[test]
    fn test_decimal256_to_float_matches_nearest(hi: i128, lo: i128, scale in 0u8..=76) {
        let mantissa = i256::from_words(hi, lo);
        proptest::prop_assert_eq!(mantissa.to_float64(scale), nearest::<f64>(mantissa, scale));
        proptest::prop_assert_eq!(mantissa.to_float32(scale), nearest::<f32>(mantissa, scale));
    }
}
