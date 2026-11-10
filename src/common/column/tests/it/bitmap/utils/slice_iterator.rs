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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::utils::SlicesIterator;
use proptest::prelude::*;

use crate::bitmap::bitmap_strategy;

proptest! {
    /// Asserts that:
    /// * `slots` is the number of set bits in the bitmap
    /// * the sum of the lens of the slices equals `slots`
    /// * each item on each slice is set
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn check_invariants(bitmap in bitmap_strategy()) {
        let iter = SlicesIterator::new(&bitmap);

        let slots = iter.slots();

        assert_eq!(bitmap.len() - bitmap.null_count(), slots);

        let slices = iter.collect::<Vec<_>>();
        let mut sum = 0;
        for (start, len) in slices {
            sum += len;
            for i in start..(start+len) {
                assert!(bitmap.get_bit(i));
            }
        }
        assert_eq!(sum, slots);
    }
}

#[test]
fn single_set() {
    let values = (0..16).map(|i| i == 1).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(1, 1)]);
    assert_eq!(count, 1);
}

#[test]
fn single_unset() {
    let values = (0..64).map(|i| i != 1).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(0, 1), (2, 62)]);
    assert_eq!(count, 64 - 1);
}

#[test]
fn generic() {
    let values = (0..130).map(|i| i % 62 != 0).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(1, 61), (63, 61), (125, 5)]);
    assert_eq!(count, 61 + 61 + 5);
}

#[test]
fn incomplete_byte() {
    let values = (0..6).map(|i| i == 1).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(1, 1)]);
    assert_eq!(count, 1);
}

#[test]
fn incomplete_byte1() {
    let values = (0..12).map(|i| i == 9).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(9, 1)]);
    assert_eq!(count, 1);
}

#[test]
fn end_of_byte() {
    let values = (0..16).map(|i| i != 7).collect::<Bitmap>();

    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    let chunks = iter.collect::<Vec<_>>();

    assert_eq!(chunks, vec![(0, 7), (8, 8)]);
    assert_eq!(count, 15);
}

#[test]
fn bla() {
    let values = vec![true, true, true, true, true, true, true, false]
        .into_iter()
        .collect::<Bitmap>();
    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    assert_eq!(values.null_count() + iter.slots(), values.len());

    let total = iter.into_iter().fold(0, |acc, x| acc + x.1);

    assert_eq!(count, total);
}

#[test]
fn past_end_should_not_be_returned() {
    let values = Bitmap::from_u8_slice([0b11111010], 3);
    let iter = SlicesIterator::new(&values);
    let count = iter.slots();
    assert_eq!(values.null_count() + iter.slots(), values.len());

    let total = iter.into_iter().fold(0, |acc, x| acc + x.1);

    assert_eq!(count, total);
}

#[test]
fn sliced() {
    let values = Bitmap::from_u8_slice([0b11111010, 0b11111011], 16);
    let values = values.sliced(8, 2);
    let iter = SlicesIterator::new(&values);

    let chunks = iter.collect::<Vec<_>>();

    // the first "11" in the second byte
    assert_eq!(chunks, vec![(0, 2)]);
}

#[test]
fn remainder_1() {
    let values = Bitmap::from_u8_slice([0, 0, 0b00000000, 0b00010101], 27);
    let values = values.sliced(22, 5);
    let iter = SlicesIterator::new(&values);
    let chunks = iter.collect::<Vec<_>>();
    assert_eq!(chunks, vec![(2, 1), (4, 1)]);
}

/// The runs of set bits of `bitmap`, one bit at a time.
fn naive_slices(bitmap: &Bitmap) -> Vec<(usize, usize)> {
    let mut slices = Vec::new();
    let mut run: Option<(usize, usize)> = None;
    for (i, set) in bitmap.iter().enumerate() {
        match (set, run) {
            (true, Some((start, len))) => run = Some((start, len + 1)),
            (true, None) => run = Some((i, 1)),
            (false, Some(r)) => {
                slices.push(r);
                run = None;
            }
            (false, None) => {}
        }
    }
    slices.extend(run);
    slices
}

fn assert_slices(bitmap: &Bitmap) {
    let iter = SlicesIterator::new(bitmap);
    assert_eq!(iter.slots(), bitmap.len() - bitmap.null_count());
    assert_eq!(iter.collect::<Vec<_>>(), naive_slices(bitmap));
}

/// Set bits around the byte and 64-bit word boundaries that the whole-byte and
/// whole-word skipping of zeros and ones steps over.
fn boundary_positions(len: usize) -> Vec<usize> {
    let mut positions = Vec::new();
    for boundary in [
        0usize, 7, 8, 63, 64, 65, 127, 128, 191, 192, 200, 511, 512, 513,
    ] {
        for p in [boundary.wrapping_sub(1), boundary, boundary + 1] {
            if p < len {
                positions.push(p);
            }
        }
    }
    positions.extend([len / 2, len - 2, len - 1].into_iter().filter(|p| *p < len));
    positions.sort_unstable();
    positions.dedup();
    positions
}

#[test]
fn sparse_runs_skip_whole_words() {
    // A few set rows in a block of 400k, as a TopN row selection.
    let len = 400_000;
    let mut set = boundary_positions(len);
    set.extend([70_001, 70_002, 70_003, 131_072, 262_143, 262_144, 399_000]);
    let bitmap = (0..len).map(|i| set.contains(&i)).collect::<Bitmap>();
    assert_slices(&bitmap);
    assert_eq!(
        SlicesIterator::new(&bitmap).count(),
        naive_slices(&bitmap).len()
    );
}

#[test]
fn dense_runs_skip_whole_words() {
    let len = 400_000;
    let unset = boundary_positions(len);
    let bitmap = (0..len).map(|i| !unset.contains(&i)).collect::<Bitmap>();
    assert_slices(&bitmap);
}

#[test]
fn uniform_bitmaps_of_every_length_near_word_boundaries() {
    for len in (1..=16).chain(56..=72).chain(120..=136).chain(504..=520) {
        assert_slices(&Bitmap::new_zeroed(len));
        assert_slices(&Bitmap::new_trued(len));
    }
}

#[test]
fn sliced_bitmaps_reach_byte_alignment_inside_words() {
    let len = 1024 + 37;
    let set = boundary_positions(len);
    let full = (0..len).map(|i| set.contains(&i)).collect::<Bitmap>();
    let dense = (0..len).map(|i| !set.contains(&i)).collect::<Bitmap>();
    for offset in [0, 1, 3, 7, 8, 9, 15, 16, 61, 64, 67, 130] {
        for sub_len in [1, 5, 8, 63, 64, 65, 200, len - offset] {
            if offset + sub_len <= len {
                assert_slices(&full.clone().sliced(offset, sub_len));
                assert_slices(&dense.clone().sliced(offset, sub_len));
            }
        }
    }
}

proptest! {
    /// The slices are the runs of set bits, for bitmaps long enough to be skipped by whole
    /// words, at any offset.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn slices_are_the_runs_of_set_bits(
        (bits, offset, len) in prop::collection::vec(prop::bool::weighted(0.02), 1..3000)
            .prop_flat_map(|bits| {
                let n = bits.len();
                (Just(bits), 0..n)
            })
            .prop_flat_map(|(bits, offset)| {
                let n = bits.len();
                (Just(bits), Just(offset), 1..=n - offset)
            })
    ) {
        let bitmap = Bitmap::from(&bits).sliced(offset, len);
        assert_slices(&bitmap);
        let bitmap = bits.iter().map(|b| !b).collect::<Bitmap>().sliced(offset, len);
        assert_slices(&bitmap);
    }
}
