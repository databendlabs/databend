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

use databend_common_expression::types::Bitmap;
use databend_common_storages_fuse::operations::read::selected_row_offsets;

fn offsets_of(bitmap: &Bitmap) -> Vec<u64> {
    selected_row_offsets(bitmap).iter().collect()
}

fn set_positions(bitmap: &Bitmap) -> Vec<u64> {
    bitmap
        .iter()
        .enumerate()
        .filter_map(|(i, set)| set.then_some(i as u64))
        .collect()
}

#[test]
fn offsets_are_the_set_rows() {
    // The rows of a block a TopN query selects: a few, far apart, some adjacent.
    let len = 300_000;
    let rows = [
        0, 1, 2, 63, 64, 65, 4_095, 4_096, 150_000, 150_001, 299_998, 299_999,
    ];
    let bitmap = (0..len).map(|i| rows.contains(&i)).collect::<Bitmap>();
    assert_eq!(offsets_of(&bitmap), rows.map(|r| r as u64));
    assert_eq!(offsets_of(&bitmap), set_positions(&bitmap));
}

#[test]
fn no_rows_and_all_rows() {
    assert!(offsets_of(&Bitmap::new_zeroed(100_000)).is_empty());
    let all = Bitmap::new_trued(100_000);
    assert_eq!(selected_row_offsets(&all).len(), 100_000);
    assert_eq!(offsets_of(&all), (0..100_000).collect::<Vec<_>>());
}

#[test]
fn sliced_selection_offsets_are_relative_to_the_slice() {
    let bitmap = (0..1000).map(|i| i % 97 == 3).collect::<Bitmap>();
    for (offset, len) in [(0, 1000), (3, 500), (100, 1), (193, 300), (999, 1)] {
        let sliced = bitmap.clone().sliced(offset, len);
        assert_eq!(
            offsets_of(&sliced),
            set_positions(&sliced),
            "offset {offset} len {len}"
        );
    }
}
