// Copyright 2022 Datafuse Labs.
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

use std::cmp::Ordering;

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;
use serde_json::json;
use serde_json::Value as JsonValue;

#[test]
fn test_variant_value_cmp() -> Result<()> {
    struct Test {
        name: &'static str,
        v1: VariantValue,
        v2s: Vec<VariantValue>,
        expects: Vec<Ordering>,
    }

    let tests = vec![
        Test {
            name: "Null",
            v1: VariantValue::from(JsonValue::Null),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i64, 2, 3])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Equal,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
            ],
        },
        Test {
            name: "Array",
            v1: VariantValue::from(json!([1i64, 2, 3])),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i64, 2, 3])),
                VariantValue::from(json!([1i64, 2, 3, 4])),
                VariantValue::from(json!([1i64])),
                VariantValue::from(json!([2i64])),
                VariantValue::from(json!(["k1", "k2"])),
                VariantValue::from(json!([true, false])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Equal,
                Ordering::Less,
                Ordering::Greater,
                Ordering::Less,
                Ordering::Less,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
            ],
        },
        Test {
            name: "Object",
            v1: VariantValue::from(json!({"a":"b","m":"n"})),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i64, 2, 3])),
                VariantValue::from(json!({"a":"b","m":"n"})),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!({"a":"c"})),
                VariantValue::from(json!({"a":"c","m":"n"})),
                VariantValue::from(json!({"a":"a"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Less,
                Ordering::Equal,
                Ordering::Greater,
                Ordering::Less,
                Ordering::Less,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
            ],
        },
        Test {
            name: "String",
            v1: VariantValue::from(json!("abcd")),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i64, 2, 3])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("abcd")),
                VariantValue::from(json!("str")),
                VariantValue::from(json!("a")),
                VariantValue::from(json!("ab")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Equal,
                Ordering::Less,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Greater,
            ],
        },
        Test {
            name: "Number-i64",
            v1: VariantValue::from(json!(12i64)),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i32, 2, 3])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(-12i64)),
                VariantValue::from(json!(100u64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Equal,
                Ordering::Greater,
                Ordering::Less,
                Ordering::Less,
                Ordering::Greater,
            ],
        },
        Test {
            name: "Number-u64",
            v1: VariantValue::from(json!(12u64)),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i32, 2, 3])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(-12i64)),
                VariantValue::from(json!(100u64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Equal,
                Ordering::Greater,
                Ordering::Less,
                Ordering::Less,
                Ordering::Greater,
            ],
        },
        Test {
            name: "Number-f64",
            v1: VariantValue::from(json!(12.34f64)),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i32, 2, 3])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(-12i64)),
                VariantValue::from(json!(100u64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Greater,
                Ordering::Greater,
                Ordering::Less,
                Ordering::Equal,
                Ordering::Greater,
            ],
        },
        Test {
            name: "Bool",
            v1: VariantValue::from(json!(true)),
            v2s: vec![
                VariantValue::from(JsonValue::Null),
                VariantValue::from(json!([1i64, 2, 3])),
                VariantValue::from(json!({"k":"v"})),
                VariantValue::from(json!("str")),
                VariantValue::from(json!(12i64)),
                VariantValue::from(json!(12.34f64)),
                VariantValue::from(json!(true)),
                VariantValue::from(json!(false)),
            ],
            expects: vec![
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Less,
                Ordering::Equal,
                Ordering::Greater,
            ],
        },
    ];

    for test in tests {
        for (v2, expect) in test.v2s.iter().zip(test.expects) {
            let res = test.v1.cmp(v2);
            assert_eq!(res, expect, "case: {:#?}", test.name);
        }
    }
    Ok(())
}

#[test]
fn test_variant_calculate_memory_size() -> Result<()> {
    let values = vec![
        VariantValue::from(JsonValue::Null),
        VariantValue::from(JsonValue::Bool(true)),
        VariantValue::from(JsonValue::Bool(false)),
        VariantValue::from(json!(100000i64)),
        VariantValue::from(json!(12.34e10f64)),
        VariantValue::from(json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")),
        VariantValue::from(json!([1i64, 2, 3, 4, 5])),
        VariantValue::from(json!({"a":"some string","b":["an","array"],"c":{"an":"object"}})),
    ];
    let expects = vec![80, 80, 80, 80, 80, 110, 480, 589];

    for (value, expect) in values.iter().zip(expects) {
        let memory_size = value.calculate_memory_size();
        assert_eq!(memory_size, expect, "value: {:?}", value);
    }
    Ok(())
}
