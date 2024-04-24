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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;

#[test]
fn as_slice() {
    let b = Bitmap::from([true, true, true, true, true, true, true, true, true]);

    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice, &[0b11111111, 0b1]);
    assert_eq!(offset, 0);
    assert_eq!(length, 9);
}

#[test]
fn as_slice_offset() {
    let b = Bitmap::from([true, true, true, true, true, true, true, true, true]);
    let b = b.sliced(8, 1);

    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice, &[0b1]);
    assert_eq!(offset, 0);
    assert_eq!(length, 1);
}

#[test]
fn as_slice_offset_middle() {
    let b = Bitmap::from_u8_slice([0, 0, 0, 0b00010101], 27);
    let b = b.sliced(22, 5);

    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice, &[0, 0b00010101]);
    assert_eq!(offset, 6);
    assert_eq!(length, 5);
}

#[test]
fn new_constant() {
    let b = Bitmap::new_constant(true, 9);
    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice[0], 0b11111111);
    assert!((slice[1] & 0b00000001) > 0);
    assert_eq!(offset, 0);
    assert_eq!(length, 9);
    assert_eq!(b.unset_bits(), 0);

    let b = Bitmap::new_constant(false, 9);
    let (slice, offset, length) = b.as_slice();
    assert_eq!(slice[0], 0b00000000);
    assert!((slice[1] & 0b00000001) == 0);
    assert_eq!(offset, 0);
    assert_eq!(length, 9);
    assert_eq!(b.unset_bits(), 9);
}

#[test]
fn debug() {
    let b = Bitmap::from([true, true, false, true, true, true, true, true, true]);
    let b = b.sliced(2, 7);

    assert_eq!(format!("{b:?}"), "[0b111110__, 0b_______1]");
}

#[test]
#[cfg(feature = "arrow")]
fn from_arrow() {
    use arrow_buffer::buffer::BooleanBuffer;
    use arrow_buffer::buffer::NullBuffer;
    let buffer = arrow_buffer::Buffer::from_iter(vec![true, true, true, false, false, false, true]);
    let bools = BooleanBuffer::new(buffer, 0, 7);
    let nulls = NullBuffer::new(bools);
    assert_eq!(nulls.null_count(), 3);

    let bitmap = Bitmap::from_null_buffer(nulls.clone());
    assert_eq!(nulls.null_count(), bitmap.unset_bits());
    assert_eq!(nulls.len(), bitmap.len());
    let back = NullBuffer::from(bitmap);
    assert_eq!(nulls, back);

    let nulls = nulls.slice(1, 3);
    assert_eq!(nulls.null_count(), 1);
    assert_eq!(nulls.len(), 3);

    let bitmap = Bitmap::from_null_buffer(nulls.clone());
    assert_eq!(nulls.null_count(), bitmap.unset_bits());
    assert_eq!(nulls.len(), bitmap.len());
    let back = NullBuffer::from(bitmap);
    assert_eq!(nulls, back);
}

#[test]
fn test_bitmap_owned_memory_usage() {
    for len in 0..100 {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let mut bitmap = Bitmap::from(vec![true; len]);

        drop(_guard);
        assert_ne!(mem_stat.get_memory_usage(), 0);
        assert_eq!(
            mem_stat.get_memory_usage(),
            bitmap.owned_memory_usage() as i64
        );
    }
}
