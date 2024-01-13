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

use databend_common_arrow::arrow::bitmap::binary_assign;
use databend_common_arrow::arrow::bitmap::unary_assign;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use proptest::prelude::*;

use crate::arrow::bitmap::bitmap_strategy;

#[test]
fn basics() {
    let mut b = MutableBitmap::from_iter(std::iter::repeat(true).take(10));
    unary_assign(&mut b, |x: u8| !x);
    assert_eq!(
        b,
        MutableBitmap::from_iter(std::iter::repeat(false).take(10))
    );

    let mut b = MutableBitmap::from_iter(std::iter::repeat(true).take(10));
    let c = Bitmap::from_iter(std::iter::repeat(true).take(10));
    binary_assign(&mut b, &c, |x: u8, y| x | y);
    assert_eq!(
        b,
        MutableBitmap::from_iter(std::iter::repeat(true).take(10))
    );
}

#[test]
fn binary_assign_oob() {
    // this check we don't have an oob access if the bitmaps are size T + 1
    // and we do some slicing.
    let a = MutableBitmap::from_iter(std::iter::repeat(true).take(65));
    let b = MutableBitmap::from_iter(std::iter::repeat(true).take(65));

    let a: Bitmap = a.into();
    let a = a.sliced(10, 20);

    let b: Bitmap = b.into();
    let b = b.sliced(10, 20);

    let mut a = a.make_mut();

    binary_assign(&mut a, &b, |x: u64, y| x & y);
}

#[test]
fn fast_paths() {
    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([true, true]);
    let b = b & &c;
    assert_eq!(b, MutableBitmap::from_iter([true, false]));

    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([false, false]);
    let b = b & &c;
    assert_eq!(b, MutableBitmap::from_iter([false, false]));

    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([true, true]);
    let b = b | &c;
    assert_eq!(b, MutableBitmap::from_iter([true, true]));

    let b = MutableBitmap::from([true, false]);
    let c = Bitmap::from_iter([false, false]);
    let b = b | &c;
    assert_eq!(b, MutableBitmap::from_iter([true, false]));
}

proptest! {
    /// Asserts that !bitmap equals all bits flipped
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn not(b in bitmap_strategy()) {
        let not_b: MutableBitmap = b.iter().map(|x| !x).collect();

        let mut b = b.make_mut();

        unary_assign(&mut b, |x: u8| !x);

        assert_eq!(b, not_b);
    }
}
