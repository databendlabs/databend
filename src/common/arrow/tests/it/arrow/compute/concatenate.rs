// Copyright 2020-2022 Jorge C. Leitão
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

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::compute::concatenate::concatenate;
use databend_common_arrow::arrow::error::Result;

#[test]
fn empty_vec() {
    let re = concatenate(&[]);
    assert!(re.is_err());
}

#[test]
fn incompatible_datatypes() {
    let re = concatenate(&[
        &Int64Array::from([Some(-1), Some(2), None]),
        &Utf8Array::<i32>::from([Some("hello"), Some("bar"), Some("world")]),
    ]);
    assert!(re.is_err());
}

#[test]
fn string_arrays() -> Result<()> {
    let arr = concatenate(&[
        &Utf8Array::<i32>::from_slice(["hello", "world"]),
        &Utf8Array::<i32>::from_slice(["2", "3", "4"]),
        &Utf8Array::<i32>::from([Some("foo"), Some("bar"), None, Some("baz")]),
    ])?;

    let expected_output = Utf8Array::<i32>::from([
        Some("hello"),
        Some("world"),
        Some("2"),
        Some("3"),
        Some("4"),
        Some("foo"),
        Some("bar"),
        None,
        Some("baz"),
    ]);

    assert_eq!(expected_output, arr.as_ref());

    Ok(())
}

#[test]
fn primitive_arrays() -> Result<()> {
    let arr = concatenate(&[
        &Int64Array::from(&[Some(-1), Some(-1), Some(2), None, None]),
        &Int64Array::from(&[Some(101), Some(102), Some(103), None]),
        &Int64Array::from(&[Some(256), Some(512), Some(1024)]),
    ])?;

    let expected_output = Int64Array::from(vec![
        Some(-1),
        Some(-1),
        Some(2),
        None,
        None,
        Some(101),
        Some(102),
        Some(103),
        None,
        Some(256),
        Some(512),
        Some(1024),
    ]);

    assert_eq!(expected_output, arr.as_ref());

    Ok(())
}

#[test]
fn primitive_array_slices() -> Result<()> {
    let input_1 = Int64Array::from(&[Some(-1), Some(-1), Some(2), None, None]).sliced(1, 3);

    let input_2 = Int64Array::from(&[Some(101), Some(102), Some(103), None]).sliced(1, 3);
    let arr = concatenate(&[&input_1, &input_2])?;

    let expected_output = Int64Array::from(&[Some(-1), Some(2), None, Some(102), Some(103), None]);

    assert_eq!(expected_output, arr.as_ref());

    Ok(())
}

#[test]
fn boolean_primitive_arrays() -> Result<()> {
    let arr = concatenate(&[
        &BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
        ]),
        &BooleanArray::from(vec![None, Some(false), Some(true), Some(false)]),
    ])?;

    let expected_output = BooleanArray::from(vec![
        Some(true),
        Some(true),
        Some(false),
        None,
        None,
        Some(false),
        None,
        Some(false),
        Some(true),
        Some(false),
    ]);

    assert_eq!(expected_output, arr.as_ref());

    Ok(())
}
