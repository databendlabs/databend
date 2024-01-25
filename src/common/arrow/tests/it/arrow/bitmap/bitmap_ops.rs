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

use databend_common_arrow::arrow::bitmap::and;
use databend_common_arrow::arrow::bitmap::or;
use databend_common_arrow::arrow::bitmap::xor;
use databend_common_arrow::arrow::bitmap::Bitmap;
use proptest::prelude::*;

use crate::arrow::bitmap::bitmap_strategy;

proptest! {
    /// Asserts that !bitmap equals all bits flipped
    #[test]
    #[cfg_attr(miri, ignore)] // miri and proptest do not work well :(
    fn not(bitmap in bitmap_strategy()) {
        let not_bitmap: Bitmap = bitmap.iter().map(|x| !x).collect();

        assert_eq!(!&bitmap, not_bitmap);
    }
}

#[test]
fn test_fast_paths() {
    let all_true = Bitmap::from(&[true, true]);
    let all_false = Bitmap::from(&[false, false]);
    let toggled = Bitmap::from(&[true, false]);

    assert_eq!(and(&all_true, &all_true), all_true);
    assert_eq!(and(&all_false, &all_true), all_false);
    assert_eq!(and(&all_true, &all_false), all_false);
    assert_eq!(and(&toggled, &all_false), all_false);
    assert_eq!(and(&toggled, &all_true), toggled);

    assert_eq!(or(&all_true, &all_true), all_true);
    assert_eq!(or(&all_true, &all_false), all_true);
    assert_eq!(or(&all_false, &all_true), all_true);
    assert_eq!(or(&all_false, &all_false), all_false);
    assert_eq!(or(&toggled, &all_false), toggled);

    assert_eq!(xor(&all_true, &all_true), all_false);
    assert_eq!(xor(&all_true, &all_false), all_true);
    assert_eq!(xor(&all_false, &all_true), all_true);
    assert_eq!(xor(&all_false, &all_false), all_false);
    assert_eq!(xor(&toggled, &toggled), all_false);
}
