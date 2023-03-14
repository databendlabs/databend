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

use common_exception::Result;
use common_expression::serialize::read_decimal;
use common_expression::serialize::read_decimal_with_size;
use common_expression::types::decimal::DecimalSize;

#[test]
fn test_decimal_text_exact() -> Result<()> {
    let cases = vec!["", ".e", ".", ".a", "-", "+", "e1", "a", "1e1e1", "1.1.1"];

    for s in cases {
        let r = read_decimal::<i128>(s.as_bytes(), 10, true);
        assert!(r.is_err(), "{s}: {r:?}");
    }
    Ok(())
}

#[test]
fn test_decimal_text() -> Result<()> {
    let cases = vec![
        ("0#", (0i128, 0, 0, 1)),
        ("+0#", (0i128, 0, 0, 2)),
        ("-0#", (0i128, 0, 0, 2)),
        ("00#", (0i128, 0, 0, 2)),
        ("0.0#", (0i128, 0, 0, 3)),
        ("00.0#", (0i128, 0, 0, 4)),
        ("00.00#", (0i128, 0, 0, 5)),
        (".0#", (0i128, 0, 0, 2)),
        (".00#", (0i128, 0, 0, 3)),
        ("0.#", (0i128, 0, 0, 2)),
        ("1#", (1i128, 1, 0, 1)),
        ("-1#", (-1i128, 1, 0, 2)),
        ("10#", (10i128, 2, 0, 2)),
        ("010.010#", (1001i128, 4, -2, 7)),
        ("0120.0210#", (120021i128, 6, -3, 9)),
        ("0120.0210e3#", (120021i128, 6, 0, 11)),
        ("0120.0210e-1#", (120021i128, 6, -4, 12)),
    ];

    for (s, l) in cases {
        let r = read_decimal::<i128>(s.as_bytes(), 10, false);
        match r {
            Ok(r) => assert_eq!(l, r, "{s}: {l:?} != {r:?}"),
            Err(e) => panic!("{s}: {l:?} != {e:?}"),
        }
    }

    let cases = vec!["", "10000000000#"];

    for s in cases {
        let r = read_decimal::<i128>(s.as_bytes(), 10, false);
        assert!(r.is_err(), "{s}: {r:?}");
    }

    Ok(())
}

#[test]
fn test_decimal_with_size_text() -> Result<()> {
    let cases = vec![
        ("0#", 0i128),
        ("+0#", 0i128),
        ("-0#", 0i128),
        ("00#", 0i128),
        ("0.0#", 0i128),
        ("00.0#", 0i128),
        ("00.00#", 0i128),
        (".0#", 0i128),
        (".00#", 0i128),
        ("0.#", 0i128),
        ("1#", 1000i128),
        ("-1#", -1000i128),
        ("10#", 10000i128),
        ("010.010#", 10010i128),
        ("0120.0210#", 120021i128),
        ("0120.0211#", 120021i128),
        (".0210#", 21i128),
        (".010#", 10i128),
        (".001#", 1i128),
        (".0210e3#", 21000i128),
        (".010e2#", 1000i128),
        ("0120.0210e-1#", 12002i128),
    ];

    let size = DecimalSize {
        precision: 6,
        scale: 3,
    };

    for (s, l) in cases {
        let r = read_decimal_with_size::<i128>(s.as_bytes(), size, false);
        match r {
            Ok(r) => assert_eq!((l, s.len() - 1), r, "{s}: {l:?} != {r:?}"),
            Err(e) => panic!("{s}: {l:?} != {e:?}"),
        }
    }

    let cases = vec!["", "10000000000#", "1e10"];

    for s in cases {
        let r = read_decimal_with_size::<i128>(s.as_bytes(), size, false);
        assert!(r.is_err(), "{s}: {r:?}");
    }

    Ok(())
}
