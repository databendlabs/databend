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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;

#[test]
fn new() {
    let buffer = Buffer::<i32>::new();
    assert_eq!(buffer.len(), 0);
    assert!(buffer.is_empty());
}

#[test]
fn from_slice() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2]);
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn slice() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2, 3]);
    let buffer = buffer.sliced(1, 2);
    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer.as_slice(), &[1, 2]);
}

#[test]
fn from_iter() {
    let buffer = (0..3).collect::<Buffer<i32>>();
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
fn debug() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2, 3]);
    let buffer = buffer.sliced(1, 2);
    let a = format!("{buffer:?}");
    assert_eq!(a, "[1, 2]")
}

#[test]
fn from_vec() {
    let buffer = Buffer::<i32>::from(vec![0, 1, 2]);
    assert_eq!(buffer.len(), 3);
    assert_eq!(buffer.as_slice(), &[0, 1, 2]);
}

#[test]
#[cfg(feature = "arrow")]
fn from_arrow() {
    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i32, 2_i32, 3_i32]);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[1, 2, 3]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);

    let buffer = buffer.slice(4);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 2);
    assert_eq!(b.as_slice(), &[2, 3]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);

    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i64, 2_i64]);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 4);
    assert_eq!(b.as_slice(), &[1, 0, 2, 0]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);

    let buffer = buffer.slice(4);
    let b = Buffer::<i32>::from(buffer.clone());
    assert_eq!(b.len(), 3);
    assert_eq!(b.as_slice(), &[0, 2, 0]);
    let back = arrow_buffer::Buffer::from(b);
    assert_eq!(back, buffer);
}

#[test]
#[cfg(feature = "arrow")]
fn from_arrow_vec() {
    // Zero-copy vec conversion in arrow-rs
    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i32, 2_i32, 3_i32]);
    let back: Vec<i32> = buffer.into_vec().unwrap();

    // Zero-copy vec conversion in arrow2
    let buffer = Buffer::<i32>::from(back);
    let back: Vec<i32> = buffer.into_mut().unwrap_right();

    let buffer = arrow_buffer::Buffer::from_vec(back);
    let buffer = Buffer::<i32>::from(buffer);

    // But not possible after conversion between buffer representations
    let _ = buffer.into_mut().unwrap_left();

    let buffer = Buffer::<i32>::from(vec![1_i32]);
    let buffer = arrow_buffer::Buffer::from(buffer);

    // But not possible after conversion between buffer representations
    let _ = buffer.into_vec::<i32>().unwrap_err();
}

#[test]
#[cfg(feature = "arrow")]
#[should_panic(expected = "not aligned")]
fn from_arrow_misaligned() {
    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i32, 2_i32, 3_i32]).slice(1);
    let _ = Buffer::<i32>::from(buffer);
}

#[test]
#[cfg(feature = "arrow")]
fn from_arrow_sliced() {
    let buffer = arrow_buffer::Buffer::from_vec(vec![1_i32, 2_i32, 3_i32]);
    let b = Buffer::<i32>::from(buffer);
    let sliced = b.sliced(1, 2);
    let back = arrow_buffer::Buffer::from(sliced);
    assert_eq!(back.typed_data::<i32>(), &[2, 3]);
}

#[test]
fn test_buffer_owned_memory_usage() {
    fn test_primitive_type<T: Copy>(index: T) {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);
        let mut buffer = Buffer::<T>::from(vec![index; 1000]);

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            buffer.owned_memory_usage() as i64
        );
    }

    test_primitive_type(0_i8);
    test_primitive_type(0_i16);
    test_primitive_type(0_i32);
    test_primitive_type(0_i64);
    test_primitive_type(0_u8);
    test_primitive_type(0_u16);
    test_primitive_type(0_u32);
    test_primitive_type(0_u64);
}
