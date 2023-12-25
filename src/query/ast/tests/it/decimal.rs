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

use databend_common_ast::ast::Literal;
use databend_common_ast::parser::expr::parse_float;
use databend_common_ast::parser::expr::parse_uint;
use ethnum::i256;

#[test]
fn test_decimal() {
    let cases = vec![
        ("1.1".to_string(), Literal::Decimal256 {
            value: 11.into(),
            precision: 76,
            scale: 1,
        }),
        ("1.1e2".to_string(), Literal::Decimal256 {
            value: 110.into(),
            precision: 76,
            scale: 0,
        }),
        ("1.1e-3".to_string(), Literal::Decimal256 {
            value: 11.into(),
            precision: 76,
            scale: 4,
        }),
        ("0.".to_string(), Literal::Decimal256 {
            value: 0.into(),
            precision: 76,
            scale: 0,
        }),
    ];

    for (i, (s, l)) in cases.iter().enumerate() {
        let r = parse_float(s);
        assert_eq!(Ok(l.clone()), r, "{i}: {s}");
    }
}

#[test]
fn test_decimal_uint() {
    let min_decimal256 = i256::from(u64::MAX) + 1;
    let float_str = "1".to_string() + &vec!["0"; 76].join("");
    let cases = vec![
        ("1".to_string(), Literal::UInt64(1)),
        (u64::MAX.to_string(), Literal::UInt64(u64::MAX)),
        (min_decimal256.to_string(), Literal::Decimal256 {
            value: min_decimal256,
            precision: 76,
            scale: 0,
        }),
        (float_str, Literal::Float64(1E76_f64)),
    ];

    for (i, (s, l)) in cases.iter().enumerate() {
        let r = parse_uint(s, 10);
        assert_eq!(Ok(l.clone()), r, "{i}: {s}");
    }
}
