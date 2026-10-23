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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::AccumulatingTransform;

use super::WindowPartitionMeta;
use super::WindowTopNSelector;
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
    partition_indices: Vec<usize>,
    sort_desc: Vec<SortColumnDescription>,
    top: usize,
    func: WindowPartitionTopNFunc,

    max_memory: usize,
    passthrough_ratio: f64,

    pending: Vec<DataBlock>,
    pending_rows: usize,
    retained: Option<DataBlock>,
    pass_rows: usize,

    input_rows: usize,
    output_rows: usize,
    passthrough: bool,
}

impl TransformWindowPartialTopN {
    pub fn new(
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        top: usize,
        func: WindowPartitionTopNFunc,
        max_memory: usize,
        passthrough_ratio: f64,
    ) -> Self {
        let sort_desc = partition_indices
            .iter()
            .map(|&offset| SortColumnDescription {
                offset,
                asc: true,
                nulls_first: false,
            })
            .chain(order_by)
            .collect();
        Self {
            partition_indices,
            sort_desc,
            top,
            func,
            max_memory,
            passthrough_ratio,
            pending: Vec::new(),
            pending_rows: 0,
            retained: None,
            pass_rows: MIN_PASS_ROWS,
            input_rows: 0,
            output_rows: 0,
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

    /// Sort the retained set together with the pending rows and keep the
    /// candidates. Returns the retained set when it has to be flushed.
    fn reduce(&mut self) -> Result<Option<DataBlock>> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        let mut blocks = Vec::with_capacity(self.pending.len() + 1);
        blocks.extend(self.retained.take());
        blocks.append(&mut self.pending);
        self.pending_rows = 0;

        let block = DataBlock::concat(&blocks)?;
        drop(blocks);

        let selector = WindowTopNSelector {
            partition_indices: &self.partition_indices,
            sort_desc: &self.sort_desc,
            top: self.top,
            func: self.func,
        };
        let selection = selector.select(&block);
        let retained = block.take(selection.rows.as_slice())?;
        drop(block);

        self.pass_rows = MIN_PASS_ROWS.max(retained.num_rows());

        if retained.memory_size() < self.max_memory {
            self.retained = Some(retained);
            return Ok(None);
        }

        // The retained set no longer fits the budget: hand it to the window
        // and decide whether reducing is still worth it.
        self.output_rows += retained.num_rows();
        let kept_ratio = self.output_rows as f64 / self.input_rows.max(1) as f64;
        if kept_ratio >= self.passthrough_ratio {
            self.passthrough = true;
        }
        Ok(Some(retained))
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
            return Ok(blocks);
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
        if !output || self.passthrough {
            self.pending.clear();
            self.retained = None;
            return Ok(vec![]);
        }

        let flushed = self.reduce()?;
        let retained = flushed.or_else(|| self.retained.take());
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

        // Later input is forwarded untouched.
        let out = t.transform(block(vec![1, 1], vec![1, 2]))?;
        assert_eq!(rows(&out), vec![(1, 1), (1, 2)]);
        assert!(t.on_finish(true)?.is_empty());
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
