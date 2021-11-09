// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_a_like_binary() -> Result<()> {
    let strings =
        DFStringArray::new_from_slice(&["Hello", "Hello", "Hello", "Hello", "He", "Hello"]);
    let patterns = DFStringArray::new_from_slice(&["H%", "W%", "%e_lo", "H_", "H_", "H\\%"]);

    let result1 = strings.a_like_binary(&patterns, |x| x).unwrap();
    let vs1: Vec<_> = result1.into_no_null_iter().collect();
    assert_eq!(vs1, [true, false, true, false, true, false]);

    let result2 = strings.a_like_binary(&patterns, |x| !x).unwrap();
    let vs2: Vec<_> = result2.into_no_null_iter().collect();
    assert_eq!(vs2, [false, true, false, true, false, true]);

    Ok(())
}

#[test]
fn test_a_like_binary_scalar() -> Result<()> {
    let strings = DFStringArray::new_from_slice(&[
        "Hello_World.",
        "Hello_World",
        "Hello World.",
        "H_llo_W%.",
        "Hello_World!",
    ]);

    let result1 = strings
        .a_like_binary_scalar("H_llo\\_W%.".as_bytes(), |x| x)
        .unwrap();
    let vs1: Vec<_> = result1.into_no_null_iter().collect();
    assert_eq!(vs1, [true, false, false, true, false]);

    let result2 = strings
        .a_like_binary_scalar("H_llo\\_W%.".as_bytes(), |x| !x)
        .unwrap();
    let vs2: Vec<_> = result2.into_no_null_iter().collect();
    assert_eq!(vs2, [false, true, true, false, true]);

    Ok(())
}

#[test]
fn test_check_pattern_type() -> Result<()> {
    struct Test {
        name: &'static str,
        pattern: &'static str,
        expect1: PatternType,
        expect2: PatternType,
    }

    let tests = vec![
        Test {
            name: "ordinal string",
            pattern: "Hello",
            expect1: PatternType::OrdinalStr,
            expect2: PatternType::OrdinalStr,
        },
        Test {
            name: "start of percent",
            pattern: "%ello",
            expect1: PatternType::StartOfPercent,
            expect2: PatternType::PatternStr,
        },
        Test {
            name: "end of percent",
            pattern: "Hell%",
            expect1: PatternType::EndOfPercent,
            expect2: PatternType::EndOfPercent,
        },
        Test {
            name: "pattern string",
            pattern: "%ell%",
            expect1: PatternType::PatternStr,
            expect2: PatternType::PatternStr,
        },
        Test {
            name: "escape percent",
            pattern: "He\\%lle",
            expect1: PatternType::PatternStr,
            expect2: PatternType::OrdinalStr,
        },
    ];

    for test in tests {
        let result1 = check_pattern_type(test.pattern.as_bytes(), false);
        assert_eq!(result1, test.expect1, "{:#?}-false", test.name);

        let result2 = check_pattern_type(test.pattern.as_bytes(), true);
        assert_eq!(result2, test.expect2, "{:#?}-true", test.name);
    }

    Ok(())
}

#[test]
fn test_like_pattern_to_regex() -> Result<()> {
    struct Test {
        name: &'static str,
        pattern: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        Test {
            name: "ordinal string",
            pattern: "Hello World",
            expect: "^Hello World$",
        },
        Test {
            name: "escape pattern",
            pattern: "Hello\\_World",
            expect: "^Hello_World$",
        },
        Test {
            name: "escape regexp pattern",
            pattern: "Hello World.",
            expect: "^Hello World\\.$",
        },
        Test {
            name: "end of backslash",
            pattern: "Hello_W%d.\\",
            expect: "^Hello.W.*d\\.\\\\$",
        },
        Test {
            name: "pattern string",
            pattern: "Hello\\._World%\\%",
            expect: "^Hello\\\\\\..World.*%$",
        },
    ];

    for test in tests {
        let result = like_pattern_to_regex(test.pattern);
        assert_eq!(&result, test.expect, "{:#?}", test.name);
    }

    Ok(())
}
