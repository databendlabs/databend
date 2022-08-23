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

#[test]
fn test_empty_string_column() {
    let mut builder = MutableStringColumn::with_values_capacity(16, 16);
    let data_column: StringColumn = builder.finish();
    let mut iter = data_column.iter();
    assert_eq!(None, iter.next());
    assert!(data_column.is_empty());
}

#[test]
fn test_new_from_slice() {
    let data_column: StringColumn = NewColumn::new_from_slice(&["你好", "hello"]);
    let mut iter = data_column.iter();
    assert_eq!("你好".as_bytes().to_vec(), iter.next().unwrap().to_vec());
    assert_eq!("hello".as_bytes().to_vec(), iter.next().unwrap().to_vec());
    assert_eq!(None, iter.next());
}

#[test]
fn test_filter_string_column() {
    const N: usize = 1024;
    let it = (0..N).map(|i| if i % 2 == 0 { "你好" } else { "hello" });
    let data_column: StringColumn = NewColumn::new_from_iter(it);
    assert!(!data_column.is_empty());
    assert!(data_column.len() == N);

    assert!(!data_column.null_at(1));

    {
        let nihao = data_column.get(512).as_string().unwrap();
        assert_eq!(nihao, "你好".as_bytes().to_vec());
    }
    let slice = data_column.slice(0, N / 2);
    assert!(slice.len() == N / 2);
}

#[test]
fn test_string_column() {
    const N: usize = 1024;
    let it = (0..N).map(|i| if i % 2 == 0 { "你好" } else { "hello" });
    let data_column: StringColumn = NewColumn::new_from_iter(it);
    struct Test {
        filter: BooleanColumn,
        expect: StringColumn,
    }

    let empty_case: Vec<&str> = vec![];
    let normal_case: Vec<&str> = (0..N)
        .map(|i| if i % 2 == 0 { "你好" } else { "hello" })
        .enumerate()
        .filter(|(i, _)| i % 3 == 0)
        .map(|(_, e)| e)
        .collect();

    let tests: Vec<Test> = vec![
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|_| true)),
            expect: NewColumn::new_from_iter((0..N).map(|i| {
                if i % 2 == 0 {
                    "你好".as_bytes()
                } else {
                    "hello".as_bytes()
                }
            })),
        },
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|_| false)),
            expect: NewColumn::new_from_iter(empty_case.iter()),
        },
        Test {
            filter: BooleanColumn::from_iterator((0..N).map(|i| i % 3 == 0)),
            expect: NewColumn::new_from_iter(normal_case.iter()),
        },
    ];

    for test in tests {
        let res = data_column.filter(&test.filter);
        let values = res
            .as_any()
            .downcast_ref::<StringColumn>()
            .unwrap()
            .values();

        assert_eq!(
            values,
            test.expect
                .as_any()
                .downcast_ref::<StringColumn>()
                .unwrap()
                .values()
        );
    }
}
