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

use databend_common_column::bitmap::utils::SlicesIterator;
use databend_common_expression::types::Bitmap;
use parquet::arrow::arrow_reader::RowSelector;

/// A wrapper around parquet's `RowSelection` that also tracks the number of selected rows (bits set to 1 in the bitmap).
#[derive(Clone)]
pub struct RowSelection {
    pub selection: parquet::arrow::arrow_reader::RowSelection,
    /// Number of selected rows (bits set to 1 in the bitmap).
    pub selected_rows: usize,
}

impl RowSelection {
    pub fn new(
        selection: parquet::arrow::arrow_reader::RowSelection,
        selected_rows: usize,
    ) -> Self {
        Self {
            selection,
            selected_rows,
        }
    }
}

impl From<&Bitmap> for RowSelection {
    fn from(bitmap: &Bitmap) -> Self {
        let mut selectors = Vec::new();
        let mut selected_rows = 0;
        let mut last_end = 0;
        for (start, len) in SlicesIterator::new(bitmap) {
            if start > last_end {
                selectors.push(RowSelector::skip(start - last_end));
            }
            if len > 0 {
                selectors.push(RowSelector::select(len));
                selected_rows += len;
            }
            last_end = start + len;
        }
        if last_end < bitmap.len() {
            selectors.push(RowSelector::skip(bitmap.len() - last_end));
        }
        Self {
            selection: parquet::arrow::arrow_reader::RowSelection::from(selectors),
            selected_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::MutableBitmap;

    use super::*;

    #[test]
    fn test_row_selection_from_bitmap() {
        let mut bitmap = MutableBitmap::from_len_zeroed(10);
        bitmap.set(0, true);
        bitmap.set(1, true);
        bitmap.set(2, false);
        bitmap.set(3, false);
        bitmap.set(4, true);
        bitmap.set(5, false);
        bitmap.set(6, true);
        bitmap.set(7, true);
        bitmap.set(8, true);
        bitmap.set(9, false);

        let bitmap: Bitmap = bitmap.into();
        let row_selection = RowSelection::from(&bitmap);
        let selectors: Vec<_> = row_selection.selection.iter().collect();

        // 6 bits are set to 1: indices 0, 1, 4, 6, 7, 8
        assert_eq!(row_selection.selected_rows, 6);
        // Expected: select(2), skip(2), select(1), skip(1), select(3), skip(1)
        assert_eq!(selectors.len(), 6);
        assert_eq!(selectors[0].row_count, 2);
        assert!(!selectors[0].skip);
        assert_eq!(selectors[1].row_count, 2);
        assert!(selectors[1].skip);
        assert_eq!(selectors[2].row_count, 1);
        assert!(!selectors[2].skip);
        assert_eq!(selectors[3].row_count, 1);
        assert!(selectors[3].skip);
        assert_eq!(selectors[4].row_count, 3);
        assert!(!selectors[4].skip);
        assert_eq!(selectors[5].row_count, 1);
        assert!(selectors[5].skip);
    }

    #[test]
    fn test_row_selection_all_selected() {
        let bitmap: Bitmap = MutableBitmap::from_len_set(5).into();
        let row_selection = RowSelection::from(&bitmap);
        let selectors: Vec<_> = row_selection.selection.iter().collect();

        // All 5 bits are set to 1
        assert_eq!(row_selection.selected_rows, 5);
        assert_eq!(selectors.len(), 1);
        assert_eq!(selectors[0].row_count, 5);
        assert!(!selectors[0].skip);
    }

    #[test]
    fn test_row_selection_none_selected() {
        let bitmap: Bitmap = MutableBitmap::from_len_zeroed(5).into();
        let row_selection = RowSelection::from(&bitmap);
        let selectors: Vec<_> = row_selection.selection.iter().collect();

        // No bits are set to 1
        assert_eq!(row_selection.selected_rows, 0);
        assert_eq!(selectors.len(), 1);
        assert_eq!(selectors[0].row_count, 5);
        assert!(selectors[0].skip);
    }
}
