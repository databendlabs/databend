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

use std::cmp::Ordering;

use crate::IndexType;

// Union two nodes vector
pub fn union(left: &[IndexType], right: &[IndexType]) -> Vec<IndexType> {
    let mut result: Vec<usize> = Vec::with_capacity(left.len() + right.len());
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            Ordering::Equal => {
                result.push(left[i]);
                i += 1;
                j += 1;
            }
            Ordering::Less => {
                result.push(left[i]);
                i += 1;
            }
            Ordering::Greater => {
                result.push(right[j]);
                j += 1;
            }
        }
    }
    if i == left.len() {
        result.extend(right[j..].iter());
    } else {
        result.extend(left[i..].iter());
    }
    result
}

pub fn intersect<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    a.iter().any(|x| b.contains(x))
}

pub fn is_subset<T: PartialEq>(v1: &[T], v2: &[T]) -> bool {
    v1.iter().all(|x| v2.contains(x))
}
