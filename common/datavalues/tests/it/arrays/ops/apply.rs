// Copyright 2020 Datafuse Labs.
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

use std::borrow::Cow;

use common_arrow::arrow::array::Array;
use common_datavalues::prelude::*;
use common_exception::Result;

fn new_test_uint16_array(cap: usize, begin: i32, end: i32) -> DFPrimitiveArray<u16> {
    let mut builder = PrimitiveArrayBuilder::<u16>::with_capacity(cap);

    (begin..end).for_each(|index| {
        if index % 3 == 0 {
            builder.append_null();
        } else {
            builder.append_value(index as u16);
        }
    });
    builder.finish()
}

fn new_test_boolean_array(cap: usize, begin: i32, end: i32) -> DFBooleanArray {
    let mut builder = BooleanArrayBuilder::with_capacity(cap);

    (begin..end).for_each(|index| {
        if index % 3 == 0 {
            builder.append_null();
        } else {
            builder.append_value(index % 3 == 1);
        }
    });
    builder.finish()
}

fn new_test_string_array(cap: usize, begin: i32, end: i32) -> DFStringArray {
    let mut builder = StringArrayBuilder::with_capacity(cap);
    let s = vec!["ax", "by", "cz", "dm", "13"];

    (begin..end).for_each(|index| {
        if index % 3 == 0 {
            builder.append_null();
        } else {
            builder.append_value(s[index as usize]);
        }
    });
    builder.finish()
}

#[test]
fn test_array_apply() -> Result<()> {
    // array=[null, 1, 2, null, 4]
    let array = new_test_uint16_array(5, 0, 5);
    let arrays = vec![
        array.apply(|arr| arr + 10),
        array.apply_with_idx(|(_, arr)| arr + 10),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(v + 20),
            None => Some(0),
        }),
        array.apply_cast_numeric(|arr| arr + 30),
        array.branch_apply_cast_numeric_no_null(|arr| match arr {
            Some(v) => (v + 40) as u16,
            None => 0_u16,
        }),
    ];

    let values = vec![
        arrays[0].inner(),
        arrays[1].inner(),
        arrays[2].inner(),
        arrays[3].inner(),
        arrays[4].inner(),
    ];

    assert_eq!(2, values[0].null_count());
    assert!(values[0].is_null(0));
    assert_eq!(11, values[0].value(1));
    assert_eq!(12, values[0].value(2));
    assert!(values[0].is_null(3));
    assert_eq!(14, values[0].value(4));

    assert_eq!(2, values[1].null_count());
    assert!(values[1].is_null(0));
    assert_eq!(11, values[1].value(1));
    assert_eq!(12, values[1].value(2));
    assert!(values[1].is_null(3));
    assert_eq!(14, values[1].value(4));

    assert_eq!(0, values[2].null_count());
    assert_eq!(0, values[2].value(0));
    assert_eq!(21, values[2].value(1));
    assert_eq!(22, values[2].value(2));
    assert_eq!(0, values[2].value(3));
    assert_eq!(24, values[2].value(4));
    assert_eq!(2, values[3].null_count());
    assert!(values[3].is_null(0));
    assert_eq!(31, values[3].value(1));
    assert_eq!(32, values[3].value(2));
    assert!(values[3].is_null(3));
    assert_eq!(34, values[3].value(4));

    assert_eq!(2, values[4].null_count());
    assert!(values[4].is_null(0));
    assert_eq!(41, values[4].value(1));
    assert_eq!(42, values[4].value(2));
    assert!(values[4].is_null(3));
    assert_eq!(44, values[4].value(4));

    Ok(())
}

#[test]
fn test_boolean_array_apply() -> Result<()> {
    // array= [null, true, false, null, true]
    let array = new_test_boolean_array(5, 0, 5);

    let arrays = vec![
        array.apply(|arr| arr),
        array.apply_with_idx(|(_, arr)| arr),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(!v),
            None => Some(true),
        }),
    ];

    let values = vec![arrays[0].inner(), arrays[1].inner(), arrays[2].inner()];

    assert_eq!(2, values[0].null_count());
    assert!(values[0].is_null(0));
    assert!(values[0].value(1));
    assert!(!values[0].value(2));
    assert!(values[0].is_null(3));
    assert!(values[0].value(4));

    assert_eq!(2, values[1].null_count());
    assert!(values[1].is_null(0));
    assert!(values[1].value(1));
    assert!(!values[1].value(2));
    assert!(values[1].is_null(3));
    assert!(values[1].value(4));

    assert_eq!(0, values[2].null_count());
    assert!(values[2].value(0));
    assert!(!values[2].value(1));
    assert!(values[2].value(2));
    assert!(values[2].value(3));
    assert!(!values[2].value(4));

    Ok(())
}

#[test]
fn test_string_array_apply() -> Result<()> {
    // array=[null, "by", "cz", null, "13"]
    let array = new_test_string_array(5, 0, 5);
    let arrays = vec![
        array.apply(|arr| Cow::from(&arr[1..])),
        array.apply_with_idx(|(_, arr)| Cow::from(&arr[..1])),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(Cow::from(&v[0..])),
            None => Some(Cow::from("ff".as_bytes())),
        }),
    ];

    let values = vec![arrays[0].inner(), arrays[1].inner(), arrays[2].inner()];

    let cast_arrays = vec![
        array.apply_cast_numeric::<_, u16>(|arr| arr.len() as u16),
        array.branch_apply_cast_numeric_no_null::<_, u16>(|arr| match arr {
            Some(v) => (v.len() + 1) as u16,
            None => 0_u16,
        }),
    ];

    let cast_values = vec![cast_arrays[0].inner(), cast_arrays[1].inner()];

    assert_eq!(2, values[0].null_count());
    assert!(values[0].is_null(0));
    assert_eq!(b"y", values[0].value(1));
    assert_eq!(b"z", values[0].value(2));
    assert!(values[0].is_null(3));
    assert_eq!(b"3", values[0].value(4));
    assert!(values[0].is_null(3));

    assert_eq!(2, values[1].null_count());
    assert!(values[1].is_null(0));
    assert_eq!(b"b", values[1].value(1));
    assert_eq!(b"c", values[1].value(2));
    assert!(values[1].is_null(3));
    assert_eq!(b"1", values[1].value(4));
    assert!(values[1].is_null(3));

    assert_eq!(0, values[2].null_count());
    assert_eq!(b"ff", values[2].value(0));
    assert_eq!(b"by", values[2].value(1));
    assert_eq!(b"cz", values[2].value(2));
    assert_eq!(b"ff", values[2].value(3));
    assert_eq!(b"13", values[2].value(4));

    assert_eq!(2, cast_values[0].null_count());
    assert!(cast_values[0].is_null(0));
    assert_eq!(2, cast_values[0].value(1));
    assert_eq!(2, cast_values[0].value(2));
    assert!(cast_values[0].is_null(3));
    assert_eq!(2, cast_values[0].value(4));

    assert_eq!(2, cast_values[1].null_count());
    assert!(cast_values[1].is_null(0));
    assert_eq!(3, cast_values[1].value(1));
    assert_eq!(3, cast_values[1].value(2));
    assert!(cast_values[1].is_null(3));
    assert_eq!(3, cast_values[1].value(4));

    Ok(())
}
