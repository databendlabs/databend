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

use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;

use crate::processors::AccumulatingTransform;

/// A sorted batch counts as effective when it drops at least this share of its rows.
///
/// The rank-limit sort is only a pre-filter for the partial aggregate: it removes rows
/// whose group keys rank past the limit. Sorting a batch costs roughly as much as
/// aggregating it, so dropping only a small share of rows does not pay for the sort.
const MIN_PRUNED_PERCENT: usize = 25;

/// Number of pass-through batches before the first re-probe after the sort is bypassed.
const INITIAL_PROBE_INTERVAL_BATCHES: usize = 4;

/// Upper bound for the pass-through run between probes.
const MAX_PROBE_INTERVAL_BATCHES: usize = 64;

enum Mode {
    /// Accumulate a batch and sort it with the rank limit.
    Sort,
    /// Forward blocks unsorted; `passed_rows` counts rows since the last probe.
    Bypass { passed_rows: usize },
}

/// Pre-filters the partial aggregate input by sorting each batch on the group keys and
/// keeping only the rows whose key ranks within the limit.
///
/// The filter can only remove rows when a batch holds more than `limit` distinct keys.
/// With low-cardinality keys nothing is ever pruned and the sort is pure overhead, so
/// the transform tracks how much each sorted batch prunes and passes blocks through
/// unsorted while the sort is ineffective. It periodically sorts one batch again, with
/// an increasing interval, so it can resume pruning if the key distribution changes.
/// The partial aggregate does not rely on sorted input, only on the pruning being
/// conservative, so bypassing never changes results. Bypassed blocks do not publish a
/// runtime TopN boundary; the boundary is only tightened by sorted batches, which keeps
/// the published value conservative.
pub struct TransformRankLimitSort {
    limit: usize,
    batch_rows: usize,
    sort_desc: Arc<[SortColumnDescription]>,
    blocks: DataBlockVec,
    rows: usize,
    runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
    mode: Mode,
    probe_interval_batches: usize,
}

impl TransformRankLimitSort {
    pub fn new(
        limit: usize,
        sort_desc: Arc<[SortColumnDescription]>,
        batch_rows: usize,
        runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
    ) -> Self {
        Self {
            limit,
            batch_rows: batch_rows.max(1),
            sort_desc,
            blocks: DataBlockVec::default(),
            rows: 0,
            runtime_top_n_filter,
            mode: Mode::Sort,
            probe_interval_batches: INITIAL_PROBE_INTERVAL_BATCHES,
        }
    }

    fn publish_runtime_top_n_boundary(&self, block: &DataBlock) {
        let Some((source_offset, filter)) = &self.runtime_top_n_filter else {
            return;
        };
        if self.limit == 0 {
            return;
        }

        let column = block.get_by_offset(*source_offset);
        let mut previous = None;
        let mut rank = 0;
        for row in 0..block.num_rows() {
            let Some(value) = column.index(row) else {
                continue;
            };
            let value = value.to_owned();
            if previous.as_ref() == Some(&value) {
                continue;
            }

            rank += 1;
            if rank == self.limit {
                filter.update(&value);
                return;
            }
            previous = Some(value);
        }
    }

    fn flush_pending(&mut self) -> Result<Option<DataBlock>> {
        if self.blocks.block_rows().is_empty() {
            return Ok(None);
        }

        let sorted = self
            .blocks
            .sort_limit(self.sort_desc.clone(), LimitType::LimitRank(self.limit))?;
        self.blocks.clear();
        self.rows = 0;
        self.publish_runtime_top_n_boundary(&sorted);

        Ok(Some(sorted))
    }

    /// Decide whether to keep sorting based on how many rows the last full batch dropped.
    fn observe_pruning(&mut self, input_rows: usize, output_rows: usize) {
        let pruned_rows = input_rows.saturating_sub(output_rows);
        if pruned_rows * 100 >= input_rows * MIN_PRUNED_PERCENT {
            self.probe_interval_batches = INITIAL_PROBE_INTERVAL_BATCHES;
            self.mode = Mode::Sort;
        } else {
            self.mode = Mode::Bypass { passed_rows: 0 };
        }
    }

    /// Returns how many of the next `num_rows` rows should be forwarded unsorted.
    ///
    /// The result is capped by the rows left before the next probe, so an oversized
    /// block is split and the probe still runs within the same block.
    fn bypass_rows(&mut self, num_rows: usize) -> usize {
        let Mode::Bypass { passed_rows } = &mut self.mode else {
            return 0;
        };

        let budget = (self.probe_interval_batches * self.batch_rows).saturating_sub(*passed_rows);
        if budget == 0 {
            // Time to probe again: sort the next batch and grow the interval so a
            // stable low-cardinality stream pays less and less for probing.
            self.probe_interval_batches =
                (self.probe_interval_batches * 2).min(MAX_PROBE_INTERVAL_BATCHES);
            self.mode = Mode::Sort;
            return 0;
        }

        let rows = num_rows.min(budget);
        *passed_rows += rows;
        rows
    }
}

impl AccumulatingTransform for TransformRankLimitSort {
    const NAME: &'static str = "TransformRankLimitSort";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        let mut output = vec![];
        loop {
            if data.is_empty() {
                return Ok(output);
            }
            let bypass_rows = self.bypass_rows(data.num_rows());
            if bypass_rows == data.num_rows() {
                output.push(data);
                return Ok(output);
            }
            if bypass_rows > 0 {
                output.push(data.slice(0..bypass_rows));
                data = data.slice(bypass_rows..data.num_rows());
                continue;
            }

            // Sort in batches of at most `batch_rows` so that a probe on a large
            // input block (a scan block can hold hundreds of thousands of rows) costs
            // no more than one regular batch. The remainder is handled by the next
            // iteration under whatever mode the probe selected.
            let capacity = self.batch_rows.saturating_sub(self.rows);
            let (batch, rest) = if data.num_rows() > capacity {
                (
                    data.slice(0..capacity),
                    data.slice(capacity..data.num_rows()),
                )
            } else {
                (data, DataBlock::empty())
            };

            self.rows += batch.num_rows();
            self.blocks.push(batch)?;

            if self.rows >= self.batch_rows {
                let input_rows = self.rows;
                if let Some(sorted) = self.flush_pending()? {
                    self.observe_pruning(input_rows, sorted.num_rows());
                    output.push(sorted);
                }
            }
            data = rest;
        }
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            Ok(self.flush_pending()?.into_iter().collect())
        } else {
            self.blocks.clear();
            self.rows = 0;
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    const LIMIT: usize = 2;
    const BATCH_ROWS: usize = 8;

    fn sort_desc() -> Arc<[SortColumnDescription]> {
        vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }]
        .into()
    }

    fn transform() -> TransformRankLimitSort {
        TransformRankLimitSort::new(LIMIT, sort_desc(), BATCH_ROWS, None)
    }

    fn block(keys: Vec<i32>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(keys)])
    }

    fn keys(block: &DataBlock) -> Vec<i32> {
        (0..block.num_rows())
            .map(|i| {
                *block
                    .get_by_offset(0)
                    .index(i)
                    .unwrap()
                    .as_number()
                    .unwrap()
                    .as_int32()
                    .unwrap()
            })
            .collect()
    }

    /// One full batch of rows with distinct keys: the rank limit prunes most of it.
    fn high_cardinality_batch() -> DataBlock {
        block((0..BATCH_ROWS as i32).rev().collect())
    }

    /// One full batch with a single key: the rank limit cannot prune anything.
    fn low_cardinality_batch() -> DataBlock {
        block(vec![7; BATCH_ROWS])
    }

    fn feed(t: &mut TransformRankLimitSort, data: DataBlock) -> Vec<DataBlock> {
        t.transform(data).unwrap()
    }

    #[test]
    fn keeps_sorting_while_pruning_is_effective() {
        let mut t = transform();
        for _ in 0..3 {
            let out = feed(&mut t, high_cardinality_batch());
            assert_eq!(out.len(), 1);
            assert_eq!(keys(&out[0]), vec![0, 1]);
            assert!(matches!(t.mode, Mode::Sort));
        }
    }

    #[test]
    fn bypasses_after_an_ineffective_batch_and_probes_again() {
        let mut t = transform();

        // The first full batch is sorted, prunes nothing, and switches to bypass.
        let out = feed(&mut t, low_cardinality_batch());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), BATCH_ROWS);
        assert!(matches!(t.mode, Mode::Bypass { .. }));

        // Bypassed blocks are forwarded immediately and untouched, even partial ones.
        for _ in 0..INITIAL_PROBE_INTERVAL_BATCHES {
            let out = feed(&mut t, block(vec![9, 3, 5, 1, 9, 3, 5, 1]));
            assert_eq!(out.len(), 1);
            assert_eq!(keys(&out[0]), vec![9, 3, 5, 1, 9, 3, 5, 1]);
        }

        // After the probe interval the next batch is accumulated and sorted again.
        let out = feed(&mut t, block(vec![7; 3]));
        assert!(out.is_empty());
        assert!(matches!(t.mode, Mode::Sort));
        let out = feed(&mut t, block(vec![7; BATCH_ROWS - 3]));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), BATCH_ROWS);

        // Still ineffective: back to bypass with a longer interval.
        assert!(matches!(t.mode, Mode::Bypass { .. }));
        assert_eq!(t.probe_interval_batches, INITIAL_PROBE_INTERVAL_BATCHES * 2);
    }

    #[test]
    fn probe_resumes_pruning_when_distribution_changes() {
        let mut t = transform();
        feed(&mut t, low_cardinality_batch());
        assert!(matches!(t.mode, Mode::Bypass { .. }));
        for _ in 0..INITIAL_PROBE_INTERVAL_BATCHES {
            feed(&mut t, low_cardinality_batch());
        }

        let out = feed(&mut t, high_cardinality_batch());
        assert_eq!(out.len(), 1);
        assert_eq!(keys(&out[0]), vec![0, 1]);
        assert!(matches!(t.mode, Mode::Sort));
        assert_eq!(t.probe_interval_batches, INITIAL_PROBE_INTERVAL_BATCHES);
    }

    #[test]
    fn probe_interval_is_capped() {
        let mut t = transform();
        // 4 + 8 + 16 + 32 + 64 pass-through batches (plus one probe each) saturate the
        // interval; keep going to make sure it never grows past the cap.
        for _ in 0..1000 {
            let out = feed(&mut t, low_cardinality_batch());
            // Whether sorted as a probe or passed through, a full batch is always
            // emitted whole because nothing can be pruned.
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].num_rows(), BATCH_ROWS);
            assert!(t.probe_interval_batches <= MAX_PROBE_INTERVAL_BATCHES);
        }
        assert!(matches!(t.mode, Mode::Bypass { .. }));
        assert_eq!(t.probe_interval_batches, MAX_PROBE_INTERVAL_BATCHES);
    }

    #[test]
    fn large_block_is_probed_one_batch_at_a_time() {
        let mut t = transform();

        // Low cardinality: only the first `BATCH_ROWS` rows are sorted as a probe,
        // the remaining rows of the same block are forwarded untouched.
        let out = feed(&mut t, block(vec![7; 3 * BATCH_ROWS + 1]));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].num_rows(), BATCH_ROWS);
        assert_eq!(out[1].num_rows(), 2 * BATCH_ROWS + 1);
        assert!(matches!(t.mode, Mode::Bypass { .. }));
        assert!(t.blocks.block_rows().is_empty());

        // High cardinality: every batch of the block is sorted and pruned.
        let mut t = transform();
        let out = feed(&mut t, block((0..3 * BATCH_ROWS as i32).rev().collect()));
        assert_eq!(out.len(), 3);
        assert_eq!(keys(&out[0]), vec![16, 17]);
        assert_eq!(keys(&out[1]), vec![8, 9]);
        assert_eq!(keys(&out[2]), vec![0, 1]);
        assert!(matches!(t.mode, Mode::Sort));

        // A block that fills an accumulated batch and spills over keeps the tail pending.
        let mut t = transform();
        assert!(feed(&mut t, block(vec![3; BATCH_ROWS - 2])).is_empty());
        let out = feed(&mut t, block((0..BATCH_ROWS as i32).collect()));
        assert_eq!(out.len(), 1);
        // 6 threes + [0, 1]: rank limit 2 keeps keys 0 and 1 only.
        assert_eq!(keys(&out[0]), vec![0, 1]);
        assert!(matches!(t.mode, Mode::Sort));
        assert_eq!(t.rows, BATCH_ROWS - 2);
        let out = t.on_finish(true).unwrap();
        assert_eq!(keys(&out[0]), vec![2, 3]);
    }

    #[test]
    fn oversized_bypassed_block_is_split_at_the_probe_budget() {
        let mut t = transform();
        feed(&mut t, low_cardinality_batch());
        assert!(matches!(t.mode, Mode::Bypass { .. }));

        // One block spans the whole bypass budget plus a high-cardinality tail. The
        // budget is forwarded untouched and the tail is probed within the same block.
        let budget = INITIAL_PROBE_INTERVAL_BATCHES * BATCH_ROWS;
        let mut data = vec![7; budget];
        data.extend((0..BATCH_ROWS as i32).rev());
        let out = feed(&mut t, block(data));
        assert_eq!(out.len(), 2);
        assert_eq!(keys(&out[0]), vec![7; budget]);
        assert_eq!(keys(&out[1]), vec![0, 1]);
        assert!(matches!(t.mode, Mode::Sort));
        assert_eq!(t.probe_interval_batches, INITIAL_PROBE_INTERVAL_BATCHES);
    }

    #[test]
    fn partial_batch_on_finish_is_sorted_and_does_not_change_mode() {
        let mut t = transform();
        let out = feed(&mut t, block(vec![5, 5, 4]));
        assert!(out.is_empty());
        let out = t.on_finish(true).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(keys(&out[0]), vec![4, 5, 5]);
        assert!(matches!(t.mode, Mode::Sort));
    }

    #[test]
    fn runtime_top_n_boundary_is_published_by_sorted_batches_only() {
        let filter = Arc::new(RuntimeTopNFilter::new(0, true, false));
        let mut t =
            TransformRankLimitSort::new(LIMIT, sort_desc(), BATCH_ROWS, Some((0, filter.clone())));

        // A single key cannot establish a rank-`LIMIT` boundary, and the sort switches
        // to bypass because nothing was pruned.
        feed(&mut t, low_cardinality_batch());
        assert!(matches!(t.mode, Mode::Bypass { .. }));
        assert_eq!(filter.boundary(), None);

        // Bypassed blocks are not sorted, so they never publish a boundary.
        for _ in 0..INITIAL_PROBE_INTERVAL_BATCHES {
            feed(&mut t, high_cardinality_batch());
        }
        assert_eq!(filter.boundary(), None);

        // The probe sorts the next batch and publishes its rank-`LIMIT` key.
        let out = feed(&mut t, block((10..10 + BATCH_ROWS as i32).rev().collect()));
        assert_eq!(out.len(), 1);
        assert_eq!(keys(&out[0]), vec![10, 11]);
        assert!(matches!(t.mode, Mode::Sort));
        assert_eq!(
            filter.boundary(),
            Some(Scalar::Number(NumberScalar::Int32(11)))
        );
    }
}
