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
fn test_empty_boolean_column() {
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
fn test_boolean_column() {
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
