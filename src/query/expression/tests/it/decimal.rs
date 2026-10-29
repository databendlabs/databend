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

use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
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

#[test]
fn test_float_to_128() {
    let cases = vec![
        (123.456, 123),
        (-123.456, -123),
        (0.0, 0),
        (0.123, 0),
        (-0.123, 0),
    ];

    for (a, b) in cases {
        let r = i128::from_float(a);
        assert_eq!(r, b);
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
