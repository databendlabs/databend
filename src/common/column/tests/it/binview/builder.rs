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

use databend_common_column::binview::BinaryViewColumnBuilder;
use databend_common_column::binview::Utf8ViewColumn;

#[test]
fn extend_from_iter() {
    let mut b = BinaryViewColumnBuilder::<str>::new();
    b.extend_trusted_len_values(vec!["a", "b"].into_iter());

    let a = b.clone();
    b.extend_trusted_len_values(a.iter());

    let b: Utf8ViewColumn = b.into();
    let c: Utf8ViewColumn =
        BinaryViewColumnBuilder::<str>::from_iter(vec!["a", "b", "a", "b"]).into();

    assert_eq!(b, c)
}

#[test]
fn new() {
    assert_eq!(BinaryViewColumnBuilder::<[u8]>::new().len(), 0);

    let a = BinaryViewColumnBuilder::<[u8]>::with_capacity(2);
    assert_eq!(a.len(), 0);
    assert_eq!(a.capacity(), 2);
}

#[test]
fn from_iter() {
    let iter = (0..3u8).map(|x| vec![x; x as usize]);
    let a: BinaryViewColumnBuilder<[u8]> = iter.clone().collect();
    let mut v_iter = a.iter();
    assert_eq!(v_iter.next(), Some(&[] as &[u8]));
    assert_eq!(v_iter.next(), Some(&[1u8] as &[u8]));
    assert_eq!(v_iter.next(), Some(&[2u8, 2] as &[u8]));

    let b = BinaryViewColumnBuilder::<[u8]>::from_iter(iter);
    assert_eq!(a.freeze(), b.freeze())
}

#[test]
fn test_pop_gc() {
    let iter = (0..1024).map(|x| format!("{}", x));
    let mut a: BinaryViewColumnBuilder<str> = iter.clone().collect();
    let item = a.pop();
    assert_eq!(item, Some("1023".to_string()));

    let column = a.freeze();
    let column = column.sliced(10, 10);
    column.gc();
}
