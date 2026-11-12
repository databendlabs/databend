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
use databend_common_catalog::plan::BlockMetaOptions;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::table_context::TableContext;
use databend_common_column::bitmap::utils::SlicesIterator;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::F32;
use databend_storages_common_pruner::BlockMetaIndex as PrunedBlockMetaIndex;
use databend_storages_common_pruner::VectorScores;
use roaring::RoaringTreemap;

use crate::FuseBlockPartInfo;
use crate::operations::BlockMetaIndex;

/// Offsets of the rows selected by a block's row selection, for the internal
/// columns (`_row_id` and the others) of the rows read. The selection is by runs
/// of set bits: a TopN query selects a few rows out of a block of hundreds of
/// thousands, and a test of every bit would cost more than reading the rows.
pub fn selected_row_offsets(selection: &Bitmap) -> RoaringTreemap {
    // The slices are in order and apart, so the offsets are appended in order.
    RoaringTreemap::from_sorted_iter(
        SlicesIterator::new(selection).flat_map(|(start, len)| start as u64..(start + len) as u64),
    )
    .unwrap()
}

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
    block_meta_options: &BlockMetaOptions,
) -> Result<DataBlock> {
    // for merge into target build
    let mut meta: Option<BlockMetaInfoPtr> =
        if block_meta_options.reserve_block_index && fuse_part.block_meta_index.is_some() {
            let block_meta_index = fuse_part.block_meta_index.as_ref().unwrap();
            Some(Box::new(BlockMetaIndex {
                segment_idx: block_meta_index.segment_idx,
                block_idx: block_meta_index.block_id,
            }))
        } else {
            None
        };

    if block_meta_options.update_stream_columns {
        // Fill `BlockMetaInfoPtr` if update stream columns
        let stream_meta = gen_mutation_stream_meta(meta, &fuse_part.location)?;
        meta = Some(Box::new(stream_meta));
    }

    if block_meta_options.query_internal_columns {
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
                    for (idx, score) in rows.into_iter().zip(scores) {
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

        let vector_scores = vector_output_scores(block_meta, offsets.as_ref());

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

/// The vector scores of the rows the scan outputs, in order. The output rows are
/// `offsets` (the rows left by filters) or, without them, every row of the block;
/// with `VectorScores::TopN`, `ReadState` keeps only its rows.
fn vector_output_scores(
    block_meta: &PrunedBlockMetaIndex,
    offsets: Option<&RoaringTreemap>,
) -> Option<Vec<F32>> {
    let output = |row: u64| offsets.is_none_or(|offsets| offsets.contains(row));
    Some(match block_meta.vector_scores.as_ref()? {
        VectorScores::TopN(scores) => {
            let mut scores = scores
                .iter()
                .filter(|(row, _)| output(u64::from(*row)))
                .copied()
                .collect::<Vec<_>>();
            scores.sort_unstable_by_key(|(row, _)| *row);
            scores.into_iter().map(|(_, score)| score).collect()
        }
        VectorScores::All(scores) => match offsets {
            Some(offsets) => offsets
                .iter()
                .map(|row| {
                    scores
                        .get(row as usize)
                        .copied()
                        .unwrap_or(F32::from(f32::MAX))
                })
                .collect(),
            None => scores.clone(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_output_scores() {
        let f = |values: &[f32]| values.iter().map(|&v| F32::from(v)).collect::<Vec<_>>();
        let offsets = RoaringTreemap::from_iter([1u64, 3]);
        let sparse = PrunedBlockMetaIndex {
            vector_scores: Some(VectorScores::TopN(vec![
                (3, F32::from(0.3)),
                (1, F32::from(0.1)),
            ])),
            ..Default::default()
        };
        // The output rows are the TopN rows, in row order, possibly fewer.
        assert_eq!(
            vector_output_scores(&sparse, Some(&offsets)),
            Some(f(&[0.1, 0.3]))
        );
        let offsets_one = RoaringTreemap::from_iter([3u64]);
        assert_eq!(
            vector_output_scores(&sparse, Some(&offsets_one)),
            Some(f(&[0.3]))
        );

        let all = PrunedBlockMetaIndex {
            vector_scores: Some(VectorScores::All(f(&[0.0, 0.1, f32::MAX, 0.3]))),
            ..Default::default()
        };
        assert_eq!(
            vector_output_scores(&all, Some(&offsets)),
            Some(f(&[0.1, 0.3]))
        );
        assert_eq!(
            vector_output_scores(&all, None),
            Some(f(&[0.0, 0.1, f32::MAX, 0.3]))
        );
        assert_eq!(vector_output_scores(&Default::default(), None), None);
    }
}
