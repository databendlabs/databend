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
use databend_storages_common_pruner::InvertedIndexMatches;
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

        let (matched_rows, matched_scores) = match &block_meta.inverted_index_matches {
            Some(matches) => {
                let (rows, scores) = inverted_index_output_matches(matches, offsets.as_ref());
                (Some(rows), scores)
            }
            None => (None, None),
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

/// The inverted index matches among the rows the scan outputs, as positions in
/// the output with their scores. The output rows are `offsets` (the rows left by
/// filters and by `BlockMetaIndex::selected_rows`) or, without them, every row of
/// the block.
fn inverted_index_output_matches(
    matches: &InvertedIndexMatches,
    offsets: Option<&RoaringTreemap>,
) -> (Vec<usize>, Option<Vec<F32>>) {
    let Some(offsets) = offsets else {
        return (matches.rows.clone(), matches.scores.clone());
    };
    debug_assert!(
        matches
            .scores
            .as_ref()
            .is_none_or(|scores| scores.len() == matches.rows.len())
    );
    let mut rows = Vec::with_capacity(matches.rows.len());
    let mut scores = matches
        .scores
        .as_ref()
        .map(|scores| Vec::with_capacity(scores.len()));
    for (pos, row) in matches.rows.iter().enumerate() {
        let row = *row as u64;
        if !offsets.contains(row) {
            continue;
        }
        // `rank` counts the output rows up to and including `row`.
        rows.push((offsets.rank(row) - 1) as usize);
        if let (Some(scores), Some(matched_scores)) = (&mut scores, &matches.scores) {
            scores.push(matched_scores[pos]);
        }
    }
    (rows, scores)
}

/// The vector scores of the rows the scan outputs, in order. The output rows are
/// `offsets` (the rows left by filters and by `BlockMetaIndex::selected_rows`)
/// or, without them, every row of the block.
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
    fn test_inverted_index_output_matches() {
        let f = |values: &[f32]| values.iter().map(|&v| F32::from(v)).collect::<Vec<_>>();
        let matches = InvertedIndexMatches {
            rows: vec![5, 1, 3],
            scores: Some(f(&[0.9, 0.5, 0.1])),
            rows_only: true,
        };
        // Every row of the block is output: the matches keep their row numbers.
        assert_eq!(
            inverted_index_output_matches(&matches, None),
            (vec![5, 1, 3], Some(f(&[0.9, 0.5, 0.1])))
        );
        // Only the matched rows are output: each match is at its rank among them.
        let offsets = RoaringTreemap::from_iter([1u64, 3, 5]);
        assert_eq!(
            inverted_index_output_matches(&matches, Some(&offsets)),
            (vec![2, 0, 1], Some(f(&[0.9, 0.5, 0.1])))
        );
        // A filter dropped row 1 and kept the unmatched row 0.
        let offsets = RoaringTreemap::from_iter([0u64, 3, 5]);
        assert_eq!(
            inverted_index_output_matches(&matches, Some(&offsets)),
            (vec![2, 1], Some(f(&[0.9, 0.1])))
        );
        let unscored = InvertedIndexMatches {
            scores: None,
            ..matches
        };
        assert_eq!(
            inverted_index_output_matches(&unscored, Some(&offsets)),
            (vec![2, 1], None)
        );
    }

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
