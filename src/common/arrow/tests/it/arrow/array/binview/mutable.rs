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

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::MutableBinaryViewArray;
use databend_common_arrow::arrow::array::Utf8ViewArray;
use databend_common_arrow::arrow::bitmap::Bitmap;

#[test]
fn new() {
    assert_eq!(MutableBinaryViewArray::<[u8]>::new().len(), 0);

    let a = MutableBinaryViewArray::<[u8]>::with_capacity(2);
    assert_eq!(a.len(), 0);
    assert_eq!(a.capacity(), 2);
}

#[test]
fn from_iter() {
    let iter = (0..3u8).map(|x| Some(vec![x; x as usize]));
    let a: MutableBinaryViewArray<[u8]> = iter.clone().collect();
    let mut v_iter = a.values_iter();
    assert_eq!(v_iter.next(), Some(&[] as &[u8]));
    assert_eq!(v_iter.next(), Some(&[1u8] as &[u8]));
    assert_eq!(v_iter.next(), Some(&[2u8, 2] as &[u8]));
    assert_eq!(a.validity(), None);

    let a = MutableBinaryViewArray::<[u8]>::from_iter(iter);
    assert_eq!(a.validity(), None);
}

#[test]
fn push_null() {
    let mut array = MutableBinaryViewArray::new();
    array.push::<&str>(None);

    let array: Utf8ViewArray = array.into();
    assert_eq!(array.validity(), Some(&Bitmap::from([false])));
}
