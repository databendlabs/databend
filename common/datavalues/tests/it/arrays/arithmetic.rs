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

use common_datavalues::prelude::*;

#[allow(dead_code)]
struct Test<T> {
    name: &'static str,
    arg_1: DFPrimitiveArray<u16>,
    arg_2: DFPrimitiveArray<u16>,
    result: Vec<Option<u16>>,
    func: fn(T, T) -> T,
}

#[test]
fn arithmetic_test() {
    let tests: Vec<Test<DFPrimitiveArray<u16>>> = vec![
        Test {
            name: "test_add",
            arg_1: DFUInt16Array::new_from_opt_slice(&[Some(5), Some(5), Some(10)]),
            arg_2: DFUInt16Array::new_from_opt_slice(&[Some(5), None, None]),
            result: vec![Some(10), None, None],
            func: |array1, array2| (&array1 + &array2).unwrap(),
        },
        Test {
            name: "test_mul",
            arg_1: DFUInt16Array::new_from_opt_slice(&[Some(2), Some(5), Some(10)]),
            arg_2: DFUInt16Array::new_from_opt_slice(&[Some(7u16), None, None]),
            result: vec![Some(14), None, None],
            func: |array1, array2| (&array1 * &array2).unwrap(),
        },
    ];
    for t in tests {
        let values = (t.func)(t.arg_1, t.arg_2);
        assert_eq!(values.collect_values(), t.result);
    }
}
