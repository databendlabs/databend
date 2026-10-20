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
            ("1.5e+1", 15000, 15000),
            ("1e-2147483648", 0, 0),
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
    }
    check::<i64>();
    check::<i128>();
    check::<i256>();
}

#[test]
fn test_decimal_with_size_invalid_text() {
    let size = DecimalSize::new_unchecked(18, 4);
    for text in ["", ".", "+", "-", ".e1", "a", "1e", "1e+", "1e-", "1ee2"] {
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
    for text in ["0x", "1e-100x", "1.2.3", "1e2e3"] {
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
