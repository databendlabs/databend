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

use std::f64::consts::PI;

use common_datavalues::prelude::*;

#[test]
fn test_empty_primitive_column() {
    let mut builder = MutablePrimitiveColumn::<i32>::with_capacity(16);
    let data_column: PrimitiveColumn<i32> = builder.finish();
    let mut iter = data_column.iter();
    assert_eq!(None, iter.next());
    assert!(data_column.is_empty());
}

#[test]
fn test_new_from_slice() {
    let data_column: PrimitiveColumn<i32> = Int32Column::from_slice(&[1, 2]);
    let mut iter = data_column.iter();
    assert_eq!(Some(&1), iter.next());
    assert_eq!(Some(&2), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_primitive_column() {
    const N: usize = 1024;
    let it = (0..N).map(|i| i as i32);
    let data_column: PrimitiveColumn<i32> = Int32Column::from_iterator(it);
    assert!(!data_column.is_empty());
    assert!(data_column.len() == N);
    assert!(!data_column.null_at(1));

    assert!(data_column.get_i64(512).unwrap() == 512);

    let slice = data_column.slice(0, N / 2);
    assert!(slice.len() == N / 2);
}

#[test]
fn test_const_column() {
    let c = ConstColumn::new(Series::from_data(vec![PI]), 24).arc();
    println!("{:?}", c);
}
