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

use databend_common_base::vec_ext::VecExt;
use databend_common_base::vec_ext::VecU8Ext;
use quickcheck::quickcheck;

#[test]
fn test_remove_first() {
    fn prop(vec: Vec<i32>, item: i32) -> bool {
        let mut vec_clone = vec.clone();
        let result = vec_clone.remove_first(&item);
        if let Some(pos) = vec.iter().position(|x| x == &item) {
            assert_eq!(result, Some(vec[pos]));
            vec_clone.remove(pos);
            vec_clone == vec_clone
        } else {
            assert_eq!(result, None);
            vec_clone == vec
        }
    }
    quickcheck(prop as fn(Vec<i32>, i32) -> bool);
}

#[test]
fn test_push_unchecked() {
    fn prop(values: Vec<i32>) -> bool {
        let mut vec: Vec<i32> = Vec::with_capacity(values.len());
        unsafe {
            for &value in &values {
                vec.push_unchecked(value);
            }
        }
        vec.len() == values.len() && vec.iter().zip(values.iter()).all(|(a, b)| a == b)
    }
    quickcheck(prop as fn(Vec<i32>) -> bool);
}

#[test]
fn test_extend_from_slice_unchecked() {
    fn prop(values: Vec<i32>) -> bool {
        let mut vec: Vec<i32> = Vec::with_capacity(values.len() + 1);
        unsafe {
            vec.push_unchecked(1); // Ensure there's at least one element
            vec.extend_from_slice_unchecked(&values);
        }
        vec.len() == 1 + values.len() && vec[1..] == values[..]
    }
    quickcheck(prop as fn(Vec<i32>) -> bool);
}

#[test]
fn test_store_value_uncheckd() {
    fn prop(value: i32) -> bool {
        let mut vec: Vec<u8> = Vec::with_capacity(std::mem::size_of::<i32>());
        unsafe {
            vec.store_value_uncheckd(&value);
        }
        vec.len() == std::mem::size_of::<i32>() && vec[0..4] == value.to_le_bytes()
    }
    quickcheck(prop as fn(i32) -> bool);
}
