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

fn create_dictionary_array(values: &[Option<&str>], keys: &[Option<i16>]) -> DictionaryArray<i16> {
    let keys = Int16Array::from(keys);
    let values = Utf8Array::<i32>::from(values);

    DictionaryArray::try_from_keys(keys, values.boxed()).unwrap()
}

#[test]
fn dictionary_equal() {
    // (a, b, c), (0, 1, 0, 2) => (a, b, a, c)
    let a = create_dictionary_array(&[Some("a"), Some("b"), Some("c")], &[
        Some(0),
        Some(1),
        Some(0),
        Some(2),
    ]);
    // different representation (values and keys are swapped), same result
    let b = create_dictionary_array(&[Some("a"), Some("c"), Some("b")], &[
        Some(0),
        Some(2),
        Some(0),
        Some(1),
    ]);
    test_equal(&a, &b, true);

    // different len
    let b = create_dictionary_array(&[Some("a"), Some("c"), Some("b")], &[
        Some(0),
        Some(2),
        Some(1),
    ]);
    test_equal(&a, &b, false);

    // different key
    let b = create_dictionary_array(&[Some("a"), Some("c"), Some("b")], &[
        Some(0),
        Some(2),
        Some(0),
        Some(0),
    ]);
    test_equal(&a, &b, false);

    // different values, same keys
    let b = create_dictionary_array(&[Some("a"), Some("b"), Some("d")], &[
        Some(0),
        Some(1),
        Some(0),
        Some(2),
    ]);
    test_equal(&a, &b, false);
}

#[test]
fn dictionary_equal_null() {
    // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
    let a = create_dictionary_array(&[Some("a"), Some("b"), Some("c")], &[
        Some(0),
        None,
        Some(0),
        Some(2),
    ]);

    // equal to self
    test_equal(&a, &a, true);

    // different representation (values and keys are swapped), same result
    let b = create_dictionary_array(&[Some("a"), Some("c"), Some("b")], &[
        Some(0),
        None,
        Some(0),
        Some(1),
    ]);
    test_equal(&a, &b, true);

    // different null position
    let b = create_dictionary_array(&[Some("a"), Some("c"), Some("b")], &[
        Some(0),
        Some(2),
        Some(0),
        None,
    ]);
    test_equal(&a, &b, false);

    // different key
    let b = create_dictionary_array(&[Some("a"), Some("c"), Some("b")], &[
        Some(0),
        None,
        Some(0),
        Some(0),
    ]);
    test_equal(&a, &b, false);

    // different values, same keys
    let b = create_dictionary_array(&[Some("a"), Some("b"), Some("d")], &[
        Some(0),
        None,
        Some(0),
        Some(2),
    ]);
    test_equal(&a, &b, false);

    // different nulls in keys and values
    let a = create_dictionary_array(&[Some("a"), Some("b"), None], &[
        Some(0),
        None,
        Some(0),
        Some(2),
    ]);
    let b = create_dictionary_array(&[Some("a"), Some("b"), Some("c")], &[
        Some(0),
        None,
        Some(0),
        None,
    ]);
    test_equal(&a, &b, true);
}
