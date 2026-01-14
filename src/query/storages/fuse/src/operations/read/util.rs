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

use std::sync::Arc;

use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::Bitmap;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use roaring::RoaringTreemap;

use crate::FuseBlockPartInfo;
use crate::operations::BlockMetaIndex;

pub fn need_reserve_block_info(ctx: Arc<dyn TableContext>, table_idx: usize) -> (bool, bool) {
    let merge_into_join = ctx.get_merge_into_join();
    (
        matches!(
            merge_into_join.merge_into_join_type,
            MergeIntoJoinType::Left
        ) && merge_into_join.target_tbl_idx == table_idx,
        merge_into_join.is_distributed,
    )
}

pub(crate) fn add_data_block_meta(
    block: DataBlock,
    fuse_part: &FuseBlockPartInfo,
    offsets: Option<RoaringTreemap>,
    base_block_ids: Option<Scalar>,
    update_stream_columns: bool,
    query_internal_columns: bool,
    need_reserve_block_info: bool,
) -> Result<DataBlock> {
    // for merge into target build
    let mut meta: Option<BlockMetaInfoPtr> =
        if need_reserve_block_info && fuse_part.block_meta_index.is_some() {
            let block_meta_index = fuse_part.block_meta_index.as_ref().unwrap();
            Some(Box::new(BlockMetaIndex {
                segment_idx: block_meta_index.segment_idx,
                block_idx: block_meta_index.block_id,
            }))
        } else {
            None
        };

    if update_stream_columns {
        // Fill `BlockMetaInfoPtr` if update stream columns
        let stream_meta = gen_mutation_stream_meta(meta, &fuse_part.location)?;
        meta = Some(Box::new(stream_meta));
    }

    if query_internal_columns {
        // Fill `BlockMetaInfoPtr` if query internal columns
        let block_meta = fuse_part.block_meta_index().unwrap();

        // Transform matched_rows indices from block-level to page-level
        let (matched_rows, matched_scores) = if let Some(offsets) = &offsets {
            match (
                block_meta.matched_rows.clone(),
                block_meta.matched_scores.clone(),
            ) {
                (Some(rows), Some(scores)) => {
                    debug_assert_eq!(rows.len(), scores.len());
                    let mut filtered_rows = Vec::with_capacity(rows.len());
                    let mut filtered_scores = Vec::with_capacity(scores.len());
                    for (idx, score) in rows.into_iter().zip(scores.into_iter()) {
                        if offsets.contains(idx as u64) {
                            let rank = offsets.rank(idx as u64);
                            debug_assert!(rank > 0);
                            let new_idx = (rank - 1) as usize;
                            filtered_rows.push(new_idx);
                            filtered_scores.push(score);
                        }
                    }
                    (Some(filtered_rows), Some(filtered_scores))
                }
                (Some(rows), None) => {
                    let mut filtered_rows = Vec::with_capacity(rows.len());
                    for idx in rows.into_iter() {
                        if offsets.contains(idx as u64) {
                            let rank = offsets.rank(idx as u64);
                            debug_assert!(rank > 0);
                            let new_idx = (rank - 1) as usize;
                            filtered_rows.push(new_idx);
                        }
                    }
                    (Some(filtered_rows), None)
                }
                (None, _) => (None, None),
            }
        } else {
            (
                block_meta.matched_rows.clone(),
                block_meta.matched_scores.clone(),
            )
        };

        // Transform vector_scores indices from block-level to page-level
        let vector_scores = block_meta.vector_scores.clone().map(|vector_scores| {
            if let Some(offsets) = &offsets {
                vector_scores
                    .into_iter()
                    .filter(|(idx, _)| offsets.contains(*idx as u64))
                    .map(|(idx, score)| ((offsets.rank(idx as u64) - 1) as usize, score))
                    .collect::<Vec<_>>()
            } else {
                vector_scores
            }
        });

        let internal_column_meta = InternalColumnMeta {
            segment_idx: block_meta.segment_idx,
            block_id: block_meta.block_id,
            block_location: block_meta.block_location.clone(),
            segment_location: block_meta.segment_location.clone(),
            snapshot_location: block_meta.snapshot_location.clone(),
            offsets,
            base_block_ids,
            inner: meta,
            matched_rows,
            matched_scores,
            vector_scores,
        };
        meta = Some(Box::new(internal_column_meta));
    }
    block.add_meta(meta)
}

pub fn bitmap_to_row_selection(bitmap: &Bitmap) -> RowSelection {
    let mut selectors = Vec::new();
    let mut i = 0;
    while i < bitmap.len() {
        let current = bitmap.get_bit(i);
        let mut run = 1;
        while i + run < bitmap.len() && bitmap.get_bit(i + run) == current {
            run += 1;
        }

        selectors.push(if current {
            RowSelector::select(run)
        } else {
            RowSelector::skip(run)
        });
        i += run;
    }
    RowSelection::from(selectors)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::MutableBitmap;

    use super::*;

    #[test]
    fn test_bitmap_to_row_selection() {
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
        let selection = bitmap_to_row_selection(&bitmap);
        let selectors: Vec<_> = selection.iter().collect();

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
    fn test_bitmap_to_row_selection_all_selected() {
        let bitmap: Bitmap = MutableBitmap::from_len_set(5).into();
        let selection = bitmap_to_row_selection(&bitmap);
        let selectors: Vec<_> = selection.iter().collect();

        assert_eq!(selectors.len(), 1);
        assert_eq!(selectors[0].row_count, 5);
        assert!(!selectors[0].skip);
    }

    #[test]
    fn test_bitmap_to_row_selection_none_selected() {
        let bitmap: Bitmap = MutableBitmap::from_len_zeroed(5).into();
        let selection = bitmap_to_row_selection(&bitmap);
        let selectors: Vec<_> = selection.iter().collect();

        assert_eq!(selectors.len(), 1);
        assert_eq!(selectors[0].row_count, 5);
        assert!(selectors[0].skip);
    }
}
