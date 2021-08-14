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

use std::fmt;

use rand::distributions::Distribution;
use rand::distributions::Standard;
use rand::random;

use crate::marshal::Marshal;
use crate::stat_buffer::StatBuffer;
use crate::unmarshal::Unmarshal;

fn test_some<T>()
where
    T: Copy + fmt::Debug + StatBuffer + Marshal + Unmarshal<T> + PartialEq,
    Standard: Distribution<T>,
{
    for _ in 0..100 {
        let mut buffer = T::buffer();
        let v = random::<T>();

        v.marshal(buffer.as_mut());
        let u = T::unmarshal(buffer.as_ref());

        assert_eq!(v, u);
    }
}

#[test]
fn test_u8() {
    test_some::<u8>()
}

#[test]
fn test_u16() {
    test_some::<u16>()
}

#[test]
fn test_u32() {
    test_some::<u32>()
}

#[test]
fn test_u64() {
    test_some::<u64>()
}

#[test]
fn test_i8() {
    test_some::<i8>()
}

#[test]
fn test_i16() {
    test_some::<i16>()
}

#[test]
fn test_i32() {
    test_some::<i32>()
}

#[test]
fn test_i64() {
    test_some::<i64>()
}

#[test]
fn test_f32() {
    test_some::<f32>()
}

#[test]
fn test_f64() {
    test_some::<f64>()
}

#[test]
fn test_bool() {
    test_some::<bool>()
}
