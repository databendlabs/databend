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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::core::VariableRowConverter;

use super::WindowPartitionMeta;
use super::WindowTopNSelector;
use super::keep_ranked;
use crate::physical_plans::WindowPartitionTopNFunc;

/// Rows accumulated before the first reduction pass; later passes wait for at
/// least as many fresh rows as the retained set holds, so the total sort work
/// stays within a constant factor of the input.
const MIN_PASS_ROWS: usize = 256 * 1024;

/// Rows per output block when a retained set is flushed downstream.
const FLUSH_BLOCK_ROWS: usize = 65536;

/// Pre-exchange reduction for `row_number()/rank()/dense_rank() <= top`
/// windows.
///
/// Every row still has to reach the node that owns its partition, but a row
/// that can no longer rank within the top `top` of its partition among the
/// rows this pipe has already seen will not rank within the top `top` of the
/// full partition either. So the transform keeps, per partition key, only the
/// rows that are still candidates and drops the rest before the shuffle. The
/// downstream window computes the real ranking over whatever survives, so
/// emitting a superset of the candidates (for example on every flush) is
/// always correct.
///
/// Rows are collected until a pass is due, then the retained set and the
/// fresh rows are sorted together by (partition keys, order keys) and
/// truncated to the candidates per partition. The pass threshold grows with
/// the retained set, which amortizes re-sorting the survivors.
///
/// When the retained set reaches `max_memory` it is flushed downstream. If by
/// then the reduction has stayed below `1 - passthrough_ratio`, the key
/// cardinality is too high for this pipe's budget and the transform switches
/// to passthrough for the rest of its input instead of paying for sorts that
/// remove nothing.
pub struct TransformWindowPartialTopN {
    /// Columns a pass compares: partition keys first, then the order keys.
    key_columns: Vec<usize>,
    /// Partition / sort descriptions over the projected key columns.
    partition_indices: Vec<usize>,
    sort_desc: Vec<SortColumnDescription>,
    top: usize,
    func: WindowPartitionTopNFunc,

    max_memory: usize,
    passthrough_ratio: f64,

    /// Row encoders over the narrow key block, built on the first merge:
    /// (full sort key, partition key only). `Some(None)` means the key types
    /// have no row encoding and passes fall back to re-sorting.
    encoders: Option<Option<(VariableRowConverter, VariableRowConverter)>>,

    pending: Vec<DataBlock>,
    pending_rows: usize,
    retained: Option<DataBlock>,
    pass_rows: usize,

    input_rows: usize,
    output_rows: usize,
    /// Consecutive passes that kept at least `passthrough_ratio` of their input.
    poor_passes: usize,
    passthrough: bool,
}

/// Rows a thread must have seen before consecutive poor passes alone (without
/// reaching the memory budget) are allowed to switch it to passthrough.
const PASSTHROUGH_SAMPLE_ROWS: usize = 3 * MIN_PASS_ROWS;

impl TransformWindowPartialTopN {
    pub fn new(
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        top: usize,
        func: WindowPartitionTopNFunc,
        max_memory: usize,
        passthrough_ratio: f64,
    ) -> Self {
        // A pass only compares the key columns, so project them into a narrow
        // block and compare there; the payload is copied once, for the rows
        // that survive.
        let key_columns = partition_indices
            .iter()
            .copied()
            .chain(order_by.iter().map(|desc| desc.offset))
            .collect::<Vec<_>>();
        let narrow_partition = (0..partition_indices.len()).collect::<Vec<_>>();
        let sort_desc = narrow_partition
            .iter()
            .map(|&offset| SortColumnDescription {
                offset,
                asc: true,
                nulls_first: false,
            })
            .chain(
                order_by
                    .iter()
                    .enumerate()
                    .map(|(i, desc)| SortColumnDescription {
                        offset: partition_indices.len() + i,
                        asc: desc.asc,
                        nulls_first: desc.nulls_first,
                    }),
            )
            .collect();
        Self {
            key_columns,
            partition_indices: narrow_partition,
            sort_desc,
            top,
            func,
            max_memory,
            passthrough_ratio,
            encoders: None,
            pending: Vec::new(),
            pending_rows: 0,
            retained: None,
            pass_rows: MIN_PASS_ROWS,
            input_rows: 0,
            output_rows: 0,
            poor_passes: 0,
            passthrough: false,
        }
    }

    pub fn is_passthrough(&self) -> bool {
        self.passthrough
    }

    fn unwrap_input(data: DataBlock) -> Vec<DataBlock> {
        if data.get_meta().is_none() {
            return if data.num_rows() > 0 {
                vec![data]
            } else {
                vec![]
            };
        }
        match data
            .get_owned_meta()
            .and_then(WindowPartitionMeta::downcast_from)
        {
            Some(meta) => meta
                .partitioned_data
                .into_iter()
                .map(|(_, block)| block)
                .filter(|block| block.num_rows() > 0)
                .collect(),
            None => vec![],
        }
    }

    /// Narrow block holding only the key columns of `block`.
    fn key_block(&self, block: &DataBlock) -> DataBlock {
        DataBlock::new(
            self.key_columns
                .iter()
                .map(|&offset| block.get_by_offset(offset).clone())
                .collect(),
            block.num_rows(),
        )
    }

    /// Sort `blocks` by the key columns and keep the candidates, copying the
    /// survivors out of `blocks` once.
    fn sort_and_truncate(&self, blocks: &[DataBlock]) -> Result<DataBlock> {
        let keys = blocks
            .iter()
            .map(|block| self.key_block(block))
            .collect::<Vec<_>>();
        let keys = DataBlock::concat(&keys)?;

        let selector = WindowTopNSelector {
            partition_indices: &self.partition_indices,
            sort_desc: &self.sort_desc,
            top: self.top,
            func: self.func,
        };
        let selection = selector.select(&keys);
        drop(keys);

        // Map the flat row numbers back to (block, row).
        let mut starts = Vec::with_capacity(blocks.len() + 1);
        starts.push(0u32);
        for block in blocks {
            starts.push(starts.last().unwrap() + block.num_rows() as u32);
        }
        let indices = selection
            .rows
            .iter()
            .map(|&row| {
                let block = starts.partition_point(|&start| start <= row) - 1;
                (block as u32, row - starts[block])
            })
            .collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(blocks, &indices))
    }

    fn encoders(
        &mut self,
        keys: &DataBlock,
    ) -> Result<Option<&(VariableRowConverter, VariableRowConverter)>> {
        if self.encoders.is_none() {
            let fields = keys
                .columns()
                .iter()
                .enumerate()
                .map(|(i, entry)| DataField::new(&i.to_string(), entry.data_type()))
                .collect::<Vec<_>>();
            let supported = fields
                .iter()
                .all(|field| VariableRowConverter::support_data_type(field.data_type()));
            let encoders = if supported {
                let schema = DataSchemaRefExt::create(fields);
                let full = VariableRowConverter::new(SortKeyDescription::new(
                    Arc::from(self.sort_desc.clone()),
                    schema.clone(),
                    false,
                )?)?;
                let partition = VariableRowConverter::new(SortKeyDescription::new(
                    Arc::from(self.sort_desc[..self.partition_indices.len()].to_vec()),
                    schema,
                    false,
                )?)?;
                Some((full, partition))
            } else {
                None
            };
            self.encoders = Some(encoders);
        }
        Ok(self.encoders.as_ref().unwrap().as_ref())
    }

    /// Append the (full sort key, partition key) row encodings of `block` as
    /// two trailing Binary columns, so later merges compare without
    /// re-encoding the retained rows. `block` is returned unchanged when the
    /// key types have no row encoding.
    fn with_encodings(&mut self, block: DataBlock) -> Result<DataBlock> {
        let keys = self.key_block(&block);
        let Some((full, partition)) = self.encoders(&keys)? else {
            return Ok(block);
        };
        let full = full.convert(&keys)?;
        let partition = partition.convert(&keys)?;
        let rows = block.num_rows();
        let mut entries = block.take_columns();
        entries.push(Column::Binary(full).into());
        entries.push(Column::Binary(partition).into());
        Ok(DataBlock::new(entries, rows))
    }

    /// Drop the trailing encoding columns added by `with_encodings`.
    fn without_encodings(&self, block: DataBlock) -> DataBlock {
        if !self.encoded() {
            return block;
        }
        let rows = block.num_rows();
        let mut entries = block.take_columns();
        entries.truncate(entries.len() - 2);
        DataBlock::new(entries, rows)
    }

    fn encoded(&self) -> bool {
        matches!(self.encoders, Some(Some(_)))
    }

    fn encoding_columns(block: &DataBlock) -> (BinaryColumn, BinaryColumn) {
        let n = block.num_columns();
        let full = block.get_by_offset(n - 2).to_column();
        let partition = block.get_by_offset(n - 1).to_column();
        (
            full.into_binary().unwrap(),
            partition.into_binary().unwrap(),
        )
    }

    /// Merge two runs that are both sorted by the key columns and carry their
    /// encodings, keeping the candidates. Linear in the two runs, so
    /// re-merging the retained set at every pass costs far less than
    /// re-sorting it. Ties keep `a` first, so the earlier arrival stays ahead
    /// as in the sorted path.
    fn merge_sorted(&mut self, a: DataBlock, b: DataBlock) -> Result<DataBlock> {
        let (full_a, part_a) = Self::encoding_columns(&a);
        let (full_b, part_b) = Self::encoding_columns(&b);

        let (len_a, len_b) = (a.num_rows(), b.num_rows());
        let mut merged: Vec<(u32, u32)> = Vec::with_capacity(len_a + len_b);
        let (mut i, mut j) = (0, 0);
        while i < len_a && j < len_b {
            if unsafe { full_a.index_unchecked(i) <= full_b.index_unchecked(j) } {
                merged.push((0, i as u32));
                i += 1;
            } else {
                merged.push((1, j as u32));
                j += 1;
            }
        }
        merged.extend((i..len_a).map(|i| (0, i as u32)));
        merged.extend((j..len_b).map(|j| (1, j as u32)));

        let full_at = |(run, idx): (u32, u32)| unsafe {
            if run == 0 {
                full_a.index_unchecked(idx as usize)
            } else {
                full_b.index_unchecked(idx as usize)
            }
        };
        let part_at = |(run, idx): (u32, u32)| unsafe {
            if run == 0 {
                part_a.index_unchecked(idx as usize)
            } else {
                part_b.index_unchecked(idx as usize)
            }
        };
        let (kept, _) = keep_ranked(
            merged.len(),
            self.top,
            self.func,
            |k| part_at(merged[k]) != part_at(merged[k - 1]),
            |k| full_at(merged[k]) != full_at(merged[k - 1]),
        );
        let indices = kept
            .into_iter()
            .map(|k| merged[k as usize])
            .collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(&[a, b], &indices))
    }

    /// Sort the pending rows, merge them into the retained set and keep the
    /// candidates. Returns the retained set when it has to be flushed.
    fn reduce(&mut self) -> Result<Option<DataBlock>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let pending = std::mem::take(&mut self.pending);
        let pass_input = self.pending_rows + self.retained.as_ref().map_or(0, DataBlock::num_rows);
        self.pending_rows = 0;

        let fresh = self.sort_and_truncate(&pending)?;
        drop(pending);
        let retained = match self.retained.take() {
            Some(retained) if self.encoded() => {
                let fresh = self.with_encodings(fresh)?;
                self.merge_sorted(retained, fresh)?
            }
            // No row encoding for these key types: re-sort both runs.
            Some(retained) => self.sort_and_truncate(&[self.without_encodings(retained), fresh])?,
            None => self.with_encodings(fresh)?,
        };

        // A pass that dropped little suggests the keys this thread has seen
        // so far do not repeat (yet). Two such passes over a large enough
        // sample mean they will not: give up before the budget is spent.
        let kept = retained.num_rows();
        let poor_pass = kept as f64 >= self.passthrough_ratio * pass_input as f64;
        if poor_pass {
            self.poor_passes += 1;
        } else {
            self.poor_passes = 0;
        }
        self.pass_rows = MIN_PASS_ROWS.max(kept);
        let sampled_enough = self.poor_passes >= 2 && self.input_rows >= PASSTHROUGH_SAMPLE_ROWS;
        if retained.memory_size() < self.max_memory && !sampled_enough {
            self.retained = Some(retained);
            return Ok(None);
        }

        // Hand the retained set to the window and decide whether reducing is
        // still worth it.
        self.output_rows += retained.num_rows();
        let kept_ratio = self.output_rows as f64 / self.input_rows.max(1) as f64;
        if kept_ratio >= self.passthrough_ratio {
            self.passthrough = true;
        }
        Ok(Some(self.without_encodings(retained)))
    }

    /// Passthrough: the local exchange hands over one small piece per
    /// partition bucket, so coalesce them into full-size blocks before the
    /// downstream exchange splits them again.
    fn forward(&mut self, blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        for block in blocks {
            self.pending_rows += block.num_rows();
            self.pending.push(block);
        }
        if self.pending_rows < FLUSH_BLOCK_ROWS {
            return Ok(vec![]);
        }
        self.take_pending()
    }

    fn take_pending(&mut self) -> Result<Vec<DataBlock>> {
        if self.pending.is_empty() {
            return Ok(vec![]);
        }
        let block = DataBlock::concat(&self.pending)?;
        self.pending.clear();
        self.pending_rows = 0;
        Ok(Self::flush_blocks(block))
    }

    fn flush_blocks(block: DataBlock) -> Vec<DataBlock> {
        if block.num_rows() <= FLUSH_BLOCK_ROWS {
            return vec![block];
        }
        block.split_by_rows_no_tail(FLUSH_BLOCK_ROWS)
    }
}

impl AccumulatingTransform for TransformWindowPartialTopN {
    const NAME: &'static str = "TransformWindowPartialTopN";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let blocks = Self::unwrap_input(data);
        if self.passthrough {
            return self.forward(blocks);
        }

        for block in blocks {
            self.pending_rows += block.num_rows();
            self.input_rows += block.num_rows();
            self.pending.push(block);
        }

        if self.pending_rows < self.pass_rows {
            return Ok(vec![]);
        }

        match self.reduce()? {
            Some(block) => Ok(Self::flush_blocks(block)),
            None => Ok(vec![]),
        }
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            self.pending.clear();
            self.retained = None;
            return Ok(vec![]);
        }
        if self.passthrough {
            return self.take_pending();
        }

        let flushed = self.reduce()?;
        let retained = match flushed {
            Some(block) => Some(block),
            None => self
                .retained
                .take()
                .map(|block| self.without_encodings(block)),
        };
        Ok(retained.map(Self::flush_blocks).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::Int32Type;

    use super::*;

    fn transform(max_memory: usize, passthrough_ratio: f64) -> TransformWindowPartialTopN {
        TransformWindowPartialTopN::new(
            vec![0],
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
            }],
            1,
            WindowPartitionTopNFunc::RowNumber,
            max_memory,
            passthrough_ratio,
        )
    }

    fn block(keys: Vec<i32>, values: Vec<i32>) -> DataBlock {
        let rows = keys.len();
        DataBlock::new(
            vec![
                Int32Type::from_data(keys).into(),
                Int32Type::from_data(values).into(),
            ],
            rows,
        )
    }

    fn rows(blocks: &[DataBlock]) -> Vec<(i32, i32)> {
        let mut rows = blocks
            .iter()
            .flat_map(|block| {
                let keys = block.get_by_offset(0).to_column();
                let values = block.get_by_offset(1).to_column();
                let keys = Int32Type::try_downcast_column(&keys).unwrap();
                let values = Int32Type::try_downcast_column(&values).unwrap();
                keys.iter()
                    .zip(values.iter())
                    .map(|(k, v)| (*k, *v))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn keeps_one_candidate_per_key_across_blocks() -> Result<()> {
        let mut t = transform(usize::MAX, 0.9);
        // The same keys show up in every block; only the max value per key
        // can be row_number() = 1 ordered by value desc.
        for round in 0..4 {
            let out = t.transform(block(vec![1, 2, 3], vec![round, 10 - round, round * 2]))?;
            assert!(out.is_empty());
        }
        let out = t.on_finish(true)?;
        assert_eq!(rows(&out), vec![(1, 3), (2, 10), (3, 6)]);
        assert!(!t.is_passthrough());
        Ok(())
    }

    #[test]
    fn reduces_when_pending_reaches_pass_threshold() -> Result<()> {
        let mut t = transform(usize::MAX, 0.9);
        let keys = (0..MIN_PASS_ROWS as i32).map(|i| i % 8).collect::<Vec<_>>();
        let values = (0..MIN_PASS_ROWS as i32).collect::<Vec<_>>();
        let out = t.transform(block(keys, values))?;
        // A pass ran but nothing had to be flushed.
        assert!(out.is_empty());
        assert_eq!(t.retained.as_ref().map(|b| b.num_rows()), Some(8));
        assert_eq!(t.pending_rows, 0);
        Ok(())
    }

    #[test]
    fn switches_to_passthrough_when_nothing_is_reduced() -> Result<()> {
        // Budget of a few bytes: the first pass flushes, and with all-distinct
        // keys the kept ratio is 1.0, above the passthrough ratio.
        let mut t = transform(1, 0.9);
        let keys = (0..MIN_PASS_ROWS as i32).collect::<Vec<_>>();
        let values = keys.clone();
        let out = t.transform(block(keys, values))?;
        assert_eq!(
            out.iter().map(DataBlock::num_rows).sum::<usize>(),
            MIN_PASS_ROWS
        );
        assert!(t.is_passthrough());

        // Later input is forwarded untouched, coalesced into full blocks.
        let out = t.transform(block(vec![1, 1], vec![1, 2]))?;
        assert!(out.is_empty());
        assert_eq!(rows(&t.on_finish(true)?), vec![(1, 1), (1, 2)]);
        Ok(())
    }

    #[test]
    fn flushes_but_keeps_reducing_when_reduction_is_good() -> Result<()> {
        let mut t = transform(1, 0.9);
        let keys = (0..MIN_PASS_ROWS as i32).map(|i| i % 8).collect::<Vec<_>>();
        let values = (0..MIN_PASS_ROWS as i32).collect::<Vec<_>>();
        let out = t.transform(block(keys, values))?;
        assert_eq!(out.iter().map(DataBlock::num_rows).sum::<usize>(), 8);
        assert!(!t.is_passthrough());
        Ok(())
    }

    fn ranking_transform(top: usize, func: WindowPartitionTopNFunc) -> TransformWindowPartialTopN {
        TransformWindowPartialTopN::new(
            vec![0],
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
            }],
            top,
            func,
            usize::MAX,
            0.9,
        )
    }

    #[test]
    fn merged_passes_match_a_single_sort() -> Result<()> {
        // Keys repeat across passes and values tie, so the merge has to get
        // partition boundaries, order ties and arrival order right.
        let n = MIN_PASS_ROWS as i32;
        let keys = (0..2 * n).map(|i| i % 97).collect::<Vec<_>>();
        let values = (0..2 * n)
            .map(|i| (i % 1009 * 7919) % 13)
            .collect::<Vec<_>>();
        for func in [
            WindowPartitionTopNFunc::RowNumber,
            WindowPartitionTopNFunc::Rank,
            WindowPartitionTopNFunc::DenseRank,
        ] {
            for top in [1, 3] {
                // One pass over everything.
                let mut single = ranking_transform(top, func);
                assert!(
                    single
                        .transform(block(keys.clone(), values.clone()))?
                        .is_empty()
                );
                let want = rows(&single.on_finish(true)?);

                // Two passes: the second merges into the retained set.
                let mut merged = ranking_transform(top, func);
                let first = block(keys[..n as usize].to_vec(), values[..n as usize].to_vec());
                let second = block(keys[n as usize..].to_vec(), values[n as usize..].to_vec());
                assert!(merged.transform(first)?.is_empty());
                assert!(merged.transform(second)?.is_empty());
                assert!(merged.encoders.as_ref().is_some_and(|e| e.is_some()));
                let got = rows(&merged.on_finish(true)?);

                match func {
                    // row_number ties are arbitrary: only the multiset of
                    // (key, value) is fixed.
                    WindowPartitionTopNFunc::RowNumber => {
                        assert_eq!(got.len(), want.len(), "{func:?} top {top}");
                        let proj = |r: &Vec<(i32, i32)>| {
                            let mut p = r.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
                            p.sort();
                            p
                        };
                        assert_eq!(proj(&got), proj(&want), "{func:?} top {top}");
                    }
                    _ => assert_eq!(got, want, "{func:?} top {top}"),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn unwraps_partitioned_input() -> Result<()> {
        let mut t = transform(usize::MAX, 0.9);
        let meta = WindowPartitionMeta::create(vec![
            (0, block(vec![1], vec![1])),
            (1, block(vec![], vec![])),
            (2, block(vec![1], vec![5])),
        ]);
        assert!(t.transform(DataBlock::empty_with_meta(meta))?.is_empty());
        let out = t.on_finish(true)?;
        assert_eq!(rows(&out), vec![(1, 5)]);
        Ok(())
    }
}
