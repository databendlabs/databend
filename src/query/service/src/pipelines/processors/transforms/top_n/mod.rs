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

mod transform_final_top_n;
mod transform_partial_top_n;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::ChunkIndex;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::Scalar;
use databend_common_pipeline_transforms::sorts::core::Rows;
use parquet::file::metadata::RowGroupMetaData;
pub use transform_final_top_n::TransformFinalTopN;
pub use transform_partial_top_n::TransformPartialTopN;

/// Metadata of one spilled candidate file, written by
/// [`TransformPartialTopN`] under memory pressure and restored at the end
/// of its input.
#[derive(Debug)]
pub struct SpilledCandidates {
    pub path: String,
    pub row_groups: Vec<RowGroupMetaData>,
}

/// The candidate set of a TopN stage.
///
/// Invariants:
/// - `current` is sorted by the order column and holds at most `capacity`
///   rows; when it is full, its last row is a valid boundary: any row that
///   sorts after it can never enter the final TopN result.
/// - `boundary` is the tightest known boundary over all sifted data,
///   including candidates that have been spilled.
/// - Candidate blocks always carry the order column at `sort_row_offset`.
pub struct TopNCandidates<R: Rows> {
    capacity: usize,
    sort_row_offset: usize,
    current: Option<(DataBlock, R)>,
    boundary: Option<Scalar>,
    /// Set when `boundary` tightens; cleared by
    /// [`Self::take_tightened_boundary`].
    boundary_tightened: bool,
}

impl<R: Rows> TopNCandidates<R> {
    pub fn new(capacity: usize, sort_row_offset: usize) -> Self {
        Self {
            capacity,
            sort_row_offset,
            current: None,
            boundary: None,
            boundary_tightened: false,
        }
    }

    /// Admit an internally sorted candidate block.
    pub fn sift_sorted(&mut self, block: DataBlock, rows: R) -> Result<()> {
        debug_assert_eq!(block.num_rows(), rows.len());
        debug_assert!(rows.is_empty() || rows.first() <= rows.last());
        if self.capacity == 0 || block.is_empty() {
            return Ok(());
        }

        let mut len = rows.len();
        if let Some(bound) = &self.boundary {
            len = prefix_within_boundary(&rows, bound);
            if len == 0 {
                return Ok(());
            }
        }
        len = len.min(self.capacity);

        if len == rows.len() {
            self.merge_sorted(block, rows)
        } else {
            let rows = rows.slice(0..len);
            self.merge_sorted(block.slice(0..len), rows)
        }
    }

    /// Admit an unsorted block: filter by the boundary, then sort (and
    /// truncate) the survivors into an internally sorted candidate block.
    pub fn sift_unsorted(&mut self, block: DataBlock, rows: R) -> Result<()> {
        debug_assert_eq!(block.num_rows(), rows.len());
        if self.capacity == 0 || block.is_empty() {
            return Ok(());
        }

        let (block, rows) = match &self.boundary {
            Some(bound) => {
                let bound_item = R::scalar_as_item(bound);
                let mut bitmap = MutableBitmap::with_capacity(rows.len());
                for i in 0..rows.len() {
                    bitmap.push(rows.row(i) <= bound_item);
                }
                let bitmap = bitmap.into();
                let block = block.filter_with_bitmap(&bitmap)?;
                if block.is_empty() {
                    return Ok(());
                }
                let rows = self.order_rows(&block)?;
                (block, rows)
            }
            None => (block, rows),
        };

        let (block, rows) = self.sort_and_truncate(block, rows)?;
        self.merge_sorted(block, rows)
    }

    /// Merge an internally sorted survivor block with the current candidates,
    /// stop after `capacity` rows, then compact the selected payload into one
    /// block. This is the only candidate replacement path.
    fn merge_sorted(&mut self, block: DataBlock, rows: R) -> Result<()> {
        debug_assert!(rows.len() <= self.capacity);
        let Some((current_block, current_rows)) = self.current.take() else {
            self.replace_current(block, rows);
            return Ok(());
        };

        let mut selected = ChunkIndex::default();
        let mut current_index = 0;
        let mut incoming_index = 0;
        while selected.num_rows() < self.capacity
            && (current_index < current_rows.len() || incoming_index < rows.len())
        {
            if incoming_index == rows.len()
                || (current_index < current_rows.len()
                    && current_rows.row(current_index) <= rows.row(incoming_index))
            {
                selected.push_merge(0, current_index as u32);
                current_index += 1;
            } else {
                selected.push_merge(1, incoming_index as u32);
                incoming_index += 1;
            }
        }

        let mut blocks = DataBlockVec::with_capacity(2);
        blocks.push(current_block)?;
        blocks.push(block)?;
        let block = blocks.take(&selected);
        let rows = self.order_rows(&block)?;
        self.replace_current(block, rows);
        Ok(())
    }

    /// Take the sorted in-memory candidates. The boundary is retained, so
    /// subsequent sifting after a spill keeps filtering.
    pub fn finish(&mut self) -> Option<(DataBlock, R)> {
        self.current.take()
    }

    /// When the boundary tightened since the last call, return the candidate
    /// block and the row index holding the boundary (its last row), so shared
    /// state is only written on change.
    pub fn take_tightened_boundary_row(&mut self) -> Option<(&DataBlock, usize)> {
        if !self.boundary_tightened {
            return None;
        }
        self.boundary_tightened = false;
        let (block, rows) = self.current.as_ref()?;
        debug_assert_eq!(rows.len(), self.capacity);
        Some((block, rows.len() - 1))
    }

    /// Absorb an externally shared boundary (the tightest bound published by
    /// any stream). It is already shared, so it is not marked as tightened
    /// for re-publication.
    pub fn tighten_boundary(&mut self, bound: Scalar) {
        debug_assert!(!matches!(bound, Scalar::Null));
        let tighter = match &self.boundary {
            Some(old) => R::scalar_as_item(&bound) < R::scalar_as_item(old),
            None => true,
        };
        if tighter {
            self.boundary = Some(bound);
        }
    }

    fn replace_current(&mut self, block: DataBlock, rows: R) {
        debug_assert!(rows.len() <= self.capacity);
        if rows.len() == self.capacity {
            let new_bound = R::owned_item(rows.last());
            let tighter = match &self.boundary {
                Some(old) => R::scalar_as_item(&new_bound) < R::scalar_as_item(old),
                None => true,
            };
            if tighter {
                self.boundary = Some(new_bound);
                self.boundary_tightened = true;
            }
        }
        self.current = Some((block, rows));
    }

    /// Sort an unsorted survivor block by the order column, keeping at most
    /// `capacity` rows. `select_nth_unstable_by` avoids sorting rows that are
    /// discarded anyway.
    fn sort_and_truncate(&self, block: DataBlock, rows: R) -> Result<(DataBlock, R)> {
        let mut permutation: Vec<u32> = (0..rows.len() as u32).collect();
        if permutation.len() > self.capacity {
            permutation.select_nth_unstable_by(self.capacity - 1, |&a, &b| {
                rows.row(a as usize).cmp(&rows.row(b as usize))
            });
            permutation.truncate(self.capacity);
        }
        permutation.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));

        let block = block.take(permutation.as_slice())?;
        let rows = self.order_rows(&block)?;
        Ok((block, rows))
    }

    fn order_rows(&self, block: &DataBlock) -> Result<R> {
        R::from_column(&block.get_by_offset(self.sort_row_offset).to_column())
    }
}

/// The number of leading rows that sort within `bound` (inclusive) in an
/// internally sorted candidate block.
fn prefix_within_boundary<R: Rows>(rows: &R, bound: &Scalar) -> usize {
    let bound = R::scalar_as_item(bound);
    if rows.first() > bound {
        return 0;
    }
    if rows.last() <= bound {
        return rows.len();
    }

    let mut left = 0;
    let mut right = rows.len();
    while left < right {
        let mid = (left + right) / 2;
        if rows.row(mid) <= bound {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

/// Split a block into chunks of at most `max_block_size` rows.
fn split_block(block: DataBlock, max_block_size: usize) -> Vec<DataBlock> {
    let num_rows = block.num_rows();
    if num_rows == 0 {
        return vec![];
    }
    let max_block_size = max_block_size.max(1);
    if num_rows <= max_block_size {
        return vec![block];
    }

    let mut blocks = Vec::with_capacity(num_rows.div_ceil(max_block_size));
    let mut offset = 0;
    while offset < num_rows {
        let end = (offset + max_block_size).min(num_rows);
        blocks.push(block.slice(offset..end));
        offset = end;
    }
    blocks
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberScalar;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsAsc;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsDesc;

    use super::*;

    fn candidate_block<R: Rows>(values: Vec<i32>) -> Result<(DataBlock, R)> {
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(values)]);
        let rows = R::from_column(&block.get_by_offset(0).to_column())?;
        Ok((block, rows))
    }

    fn int32_values(block: &DataBlock) -> Vec<i32> {
        (0..block.num_rows())
            .map(|row| match block.get_by_offset(0).index(row).unwrap() {
                ScalarRef::Number(NumberScalar::Int32(value)) => value,
                value => panic!("expected Int32 candidate, got {value:?}"),
            })
            .collect()
    }

    #[test]
    fn test_zero_capacity_keeps_no_candidates() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsAsc<Int32Type>>::new(0, 0);
        let (block, rows) = candidate_block(vec![3, 1, 2])?;
        candidates.sift_unsorted(block, rows)?;

        assert!(candidates.finish().is_none());
        assert!(candidates.boundary.is_none());
        Ok(())
    }

    #[test]
    fn test_tighten_boundary_absorbs_external_bounds() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsAsc<Int32Type>>::new(3, 0);
        candidates.tighten_boundary(Scalar::Number(NumberScalar::Int32(5)));
        // External boundaries are not publishable changes.
        assert!(candidates.take_tightened_boundary_row().is_none());

        // Rows beyond the absorbed boundary are filtered out.
        let (block, rows) = candidate_block(vec![7, 1, 6, 2])?;
        candidates.sift_unsorted(block, rows)?;
        let (block, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&block), vec![1, 2]);

        // A looser external boundary must not weaken the local one.
        candidates.tighten_boundary(Scalar::Number(NumberScalar::Int32(9)));
        let (block, rows) = candidate_block(vec![8])?;
        candidates.sift_unsorted(block, rows)?;
        assert!(candidates.finish().is_none());

        // Descending candidates absorb with the reversed ordering.
        let mut desc = TopNCandidates::<SimpleRowsDesc<Int32Type>>::new(3, 0);
        desc.tighten_boundary(Scalar::Number(NumberScalar::Int32(5)));
        let (block, rows) = candidate_block(vec![9, 3, 7])?;
        desc.sift_unsorted(block, rows)?;
        let (block, _) = desc.finish().unwrap();
        assert_eq!(int32_values(&block), vec![9, 7]);
        Ok(())
    }

    #[test]
    fn test_take_tightened_boundary_only_reports_changes() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsAsc<Int32Type>>::new(2, 0);

        // Not full yet: no boundary to publish.
        let (block, rows) = candidate_block(vec![5])?;
        candidates.sift_unsorted(block, rows)?;
        assert!(candidates.take_tightened_boundary_row().is_none());

        // Reaching capacity establishes the boundary exactly once.
        let (block, rows) = candidate_block(vec![9, 7])?;
        candidates.sift_unsorted(block, rows)?;
        {
            let (block, row) = candidates.take_tightened_boundary_row().unwrap();
            assert_eq!(int32_values(block)[row], 7);
        }
        assert!(candidates.take_tightened_boundary_row().is_none());

        // A block that cannot tighten the boundary reports no change.
        let (block, rows) = candidate_block(vec![8])?;
        candidates.sift_unsorted(block, rows)?;
        assert!(candidates.take_tightened_boundary_row().is_none());

        // A better row tightens the boundary again.
        let (block, rows) = candidate_block(vec![1])?;
        candidates.sift_unsorted(block, rows)?;
        {
            let (block, row) = candidates.take_tightened_boundary_row().unwrap();
            assert_eq!(int32_values(block)[row], 5);
        }
        Ok(())
    }

    #[test]
    fn test_unsorted_blocks_are_truncated_and_bounded_merged() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsAsc<Int32Type>>::new(4, 0);

        let (block, rows) = candidate_block(vec![9, 1, 5, 3, 7])?;
        candidates.sift_unsorted(block, rows)?;
        let (block, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&block), vec![1, 3, 5, 7]);

        // `finish` models a spill: in-memory candidates are released, but the
        // known boundary remains and filters the next unsorted block.
        let (new_block, new_rows) = candidate_block(vec![100, 6, 0, 7, 2])?;
        candidates.sift_unsorted(new_block, new_rows)?;
        let (new_block, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&new_block), vec![0, 2, 6, 7]);

        // Restoring both sorted candidate blocks must recover the global TopN.
        let old_rows =
            SimpleRowsAsc::<Int32Type>::from_column(&block.get_by_offset(0).to_column())?;
        candidates.sift_sorted(block, old_rows)?;
        let new_rows =
            SimpleRowsAsc::<Int32Type>::from_column(&new_block.get_by_offset(0).to_column())?;
        candidates.sift_sorted(new_block, new_rows)?;

        let (result, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&result), vec![0, 1, 2, 3]);
        Ok(())
    }

    #[test]
    fn test_sorted_admission_uses_inclusive_boundary_prefix() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsAsc<Int32Type>>::new(3, 0);
        let (spilled, spilled_rows) = candidate_block(vec![1, 2, 3])?;
        candidates.sift_sorted(spilled, spilled_rows)?;
        let (spilled, _) = candidates.finish().unwrap();

        // Values after 3 are outside the retained boundary; an equal boundary
        // value remains admissible so ties are not incorrectly discarded.
        let (block, rows) = candidate_block(vec![2, 3, 4, 5])?;
        candidates.sift_sorted(block, rows)?;
        let (block, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&block), vec![2, 3]);

        let spilled_rows =
            SimpleRowsAsc::<Int32Type>::from_column(&spilled.get_by_offset(0).to_column())?;
        candidates.sift_sorted(spilled, spilled_rows)?;
        let rows = SimpleRowsAsc::<Int32Type>::from_column(&block.get_by_offset(0).to_column())?;
        candidates.sift_sorted(block, rows)?;
        let (result, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&result), vec![1, 2, 2]);
        Ok(())
    }

    #[test]
    fn test_duplicate_keys_across_blocks() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsAsc<Int32Type>>::new(4, 0);
        for values in [vec![4, 2, 2, 1], vec![3, 2, 2, 5]] {
            let (block, rows) = candidate_block(values)?;
            candidates.sift_unsorted(block, rows)?;
        }

        let (result, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&result), vec![1, 2, 2, 2]);
        Ok(())
    }

    #[test]
    fn test_descending_candidates_follow_rows_order() -> Result<()> {
        let mut candidates = TopNCandidates::<SimpleRowsDesc<Int32Type>>::new(3, 0);
        for values in [vec![1, 9, 3], vec![8, 10, 2]] {
            let (block, rows) = candidate_block(values)?;
            candidates.sift_unsorted(block, rows)?;
        }

        let (result, _) = candidates.finish().unwrap();
        assert_eq!(int32_values(&result), vec![10, 9, 8]);
        Ok(())
    }
}
