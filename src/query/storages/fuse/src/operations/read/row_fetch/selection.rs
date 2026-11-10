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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::MutableBitmap;

use crate::io::RowSelection;

/// The rows to decode from a block for a fetch. A fetch usually wants a few rows
/// of a block, and most of the decoding cost is in the values, not in the
/// decompression of their pages, so the other rows are skipped.
pub(super) struct BlockRowsSelection {
    /// The requested rows, `None` if that is all rows of the block.
    pub(super) selection: Option<RowSelection>,
    /// The rows to take from the decoded block, one for each requested row, in
    /// the requested order. Decoding yields each selected row once, in row order,
    /// while requests come in any order and may repeat a row: a join can match
    /// one fetched row many times.
    take_indices: Vec<u32>,
}

impl BlockRowsSelection {
    pub(super) fn try_create(num_rows: usize, take_indices: Vec<u32>) -> Result<Self> {
        let mut rows = take_indices.clone();
        rows.sort_unstable();
        rows.dedup();

        if let Some(row) = rows.last().filter(|row| **row as usize >= num_rows) {
            return Err(ErrorCode::Internal(format!(
                "RowID is invalid, row idx {row}, block rows {num_rows}"
            )));
        }

        if rows.len() == num_rows {
            return Ok(BlockRowsSelection {
                selection: None,
                take_indices,
            });
        }

        let mut bitmap = MutableBitmap::from_len_zeroed(num_rows);
        for row in &rows {
            bitmap.set(*row as usize, true);
        }
        let bitmap: Bitmap = bitmap.into();

        let take_indices = take_indices
            .iter()
            // `rows` holds every requested row.
            .map(|row| rows.binary_search(row).unwrap() as u32)
            .collect();

        Ok(BlockRowsSelection {
            selection: Some(RowSelection::from(&bitmap)),
            take_indices,
        })
    }

    /// Takes the requested rows of a decoded block. The strings of a sparse decode
    /// point into whole decompressed pages, and `Assemble` keeps the block until its
    /// batch is complete, so they are copied out to release the pages.
    pub(super) fn take_decoded_rows(&self, block: &DataBlock) -> Result<DataBlock> {
        let indices = self.take_indices.as_slice();
        match self.selection {
            Some(_) => block.take_with_optimize_size(indices),
            None => block.take(indices),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::StringType;

    use super::*;

    #[test]
    fn test_block_rows_selection_maps_requested_rows() -> Result<()> {
        // Out of order, and row 7 is requested twice.
        let rows = BlockRowsSelection::try_create(10, vec![7, 2, 7, 5])?;

        let selection = rows.selection.unwrap();
        assert_eq!(selection.selected_rows, 3);
        assert_eq!(selection.bitmap.len(), 10);
        let selected = selection
            .bitmap
            .iter()
            .enumerate()
            .filter_map(|(row, selected)| selected.then_some(row))
            .collect::<Vec<_>>();
        assert_eq!(selected, vec![2, 5, 7]);

        // Row 7 is the third decoded row, row 2 the first and row 5 the second.
        assert_eq!(rows.take_indices, vec![2, 0, 2, 1]);
        Ok(())
    }

    #[test]
    fn test_block_rows_selection_all_rows_decodes_block() -> Result<()> {
        let rows = BlockRowsSelection::try_create(3, vec![2, 0, 1, 0])?;

        assert!(rows.selection.is_none());
        assert_eq!(rows.take_indices, vec![2, 0, 1, 0]);
        Ok(())
    }

    #[test]
    fn test_block_rows_selection_sparse_decode_releases_pages() -> Result<()> {
        // Two rows decoded from a page: their strings point into the whole page.
        let page = StringType::from_data(vec!["x".repeat(1024); 1024]);
        let decoded = DataBlock::new_from_columns(vec![page.slice(0..2)]);
        let total_buffer_len = |block: &DataBlock| {
            let column = block.get_by_offset(0).to_column();
            column.as_string().unwrap().total_buffer_len()
        };
        assert!(total_buffer_len(&decoded) >= 1024 * 1024);

        let rows = BlockRowsSelection::try_create(1024, vec![1, 0])?;
        let taken = rows.take_decoded_rows(&decoded)?;

        assert_eq!(taken.num_rows(), 2);
        assert!(total_buffer_len(&taken) <= 2 * 1024);
        Ok(())
    }

    #[test]
    fn test_block_rows_selection_rejects_row_out_of_block() {
        let result = BlockRowsSelection::try_create(3, vec![1, 3]);

        assert!(result.err().unwrap().message().contains("RowID is invalid"));
    }
}
