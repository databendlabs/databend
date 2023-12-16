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

use super::test_equal;

#[test]
fn test_boolean_equal() {
    let a = BooleanArray::from_slice([false, false, true]);
    let b = BooleanArray::from_slice([false, false, true]);
    test_equal(&a, &b, true);

    let b = BooleanArray::from_slice([false, false, false]);
    test_equal(&a, &b, false);
}

#[test]
fn test_boolean_equal_null() {
    let a = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
    let b = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
    test_equal(&a, &b, true);

    let b = BooleanArray::from(vec![None, None, None, Some(true)]);
    test_equal(&a, &b, false);

    let b = BooleanArray::from(vec![Some(true), None, None, Some(true)]);
    test_equal(&a, &b, false);
}

#[test]
fn test_boolean_equal_offset() {
    let a = BooleanArray::from_slice(vec![false, true, false, true, false, false, true]);
    let b = BooleanArray::from_slice(vec![true, false, false, false, true, false, true, true]);
    test_equal(&a, &b, false);

    let a_slice = a.sliced(2, 3);
    let b_slice = b.sliced(3, 3);
    test_equal(&a_slice, &b_slice, true);

    let a_slice = a.sliced(3, 4);
    let b_slice = b.sliced(4, 4);
    test_equal(&a_slice, &b_slice, false);

    // Elements fill in `u8`'s exactly.
    let mut vector = vec![false, false, true, true, true, true, true, true];
    let a = BooleanArray::from_slice(vector.clone());
    let b = BooleanArray::from_slice(vector.clone());
    test_equal(&a, &b, true);

    // Elements fill in `u8`s + suffix bits.
    vector.push(true);
    let a = BooleanArray::from_slice(vector.clone());
    let b = BooleanArray::from_slice(vector);
    test_equal(&a, &b, true);
}
