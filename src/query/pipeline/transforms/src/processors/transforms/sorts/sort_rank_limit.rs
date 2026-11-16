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

use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_catalog::runtime_filter_info::RuntimeTopNBoundary;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::LimitType;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256Type;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::with_number_mapped_type;

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
///
/// Rows are also checked against a boundary on the first key before they are sorted.
/// When a sorted batch holds at least `limit` distinct keys, its last kept row has the
/// `limit`-th one, so `limit` distinct keys have a first key that is not worse than the
/// first key `b` of that row. A row whose first key is strictly worse than `b` ranks after
/// all of them whatever its later keys are, so it can be dropped; rows tied with `b` are
/// kept. Every such `b` is valid on its own, so the boundary shared by all streams and the
/// scans stays valid whichever one it keeps. Once the boundary is tight, a batch costs a
/// comparison per row instead of a sort.
pub struct TransformRankLimitSort {
    limit: usize,
    batch_rows: usize,
    sort_desc: Arc<[SortColumnDescription]>,
    blocks: DataBlockVec,
    /// Input rows of the pending batch, before the boundary dropped some of them.
    input_rows: usize,
    /// Shared by all streams of the aggregate, and by the scans that prune by it.
    boundary: Arc<RuntimeTopNBoundary>,
    mode: Mode,
    probe_interval_batches: usize,
}

impl TransformRankLimitSort {
    pub fn new(
        limit: usize,
        sort_desc: Arc<[SortColumnDescription]>,
        batch_rows: usize,
        boundary: Arc<RuntimeTopNBoundary>,
    ) -> Self {
        Self {
            limit,
            batch_rows: batch_rows.max(1),
            sort_desc,
            blocks: DataBlockVec::default(),
            input_rows: 0,
            boundary,
            mode: Mode::Sort,
            probe_interval_batches: INITIAL_PROBE_INTERVAL_BATCHES,
        }
    }

    /// Drop the rows of `block` whose first key is strictly worse than the boundary.
    fn filter_by_boundary(&self, block: DataBlock) -> Result<DataBlock> {
        let Some(boundary) = self.boundary.boundary() else {
            return Ok(block);
        };
        let desc = &self.sort_desc[0];
        match rows_within_boundary(block.get_by_offset(desc.offset), &boundary, desc) {
            Some(keep) if keep.null_count() > 0 => block.filter_with_bitmap(&keep),
            _ => Ok(block),
        }
    }

    fn flush_pending(&mut self) -> Result<Option<DataBlock>> {
        if self.blocks.block_rows().is_empty() {
            return Ok(None);
        }

        let rows: usize = self.blocks.block_rows().iter().sum();
        let sorted = self
            .blocks
            .sort_limit(self.sort_desc.clone(), LimitType::LimitRank(self.limit))?;
        self.blocks.clear();
        self.input_rows = 0;

        // The last row has the `limit`-th distinct key when the rank limit cut the batch,
        // which then held more than `limit` keys, or when it kept a batch with at least,
        // and so exactly, `limit` keys.
        let first_key = sorted.get_by_offset(self.sort_desc[0].offset);
        if (sorted.num_rows() < rows || has_distinct_keys(&sorted, &self.sort_desc, self.limit))
            && let Some(last_row) = sorted.num_rows().checked_sub(1)
            && let Some(value) = first_key.index(last_row)
        {
            // A null first key bounds nothing, `update` ignores it.
            self.boundary.update(&value.to_owned());
        }

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
            let capacity = self.batch_rows.saturating_sub(self.input_rows);
            let (batch, rest) = if data.num_rows() > capacity {
                (
                    data.slice(0..capacity),
                    data.slice(capacity..data.num_rows()),
                )
            } else {
                (data, DataBlock::empty())
            };

            // Batches are counted in input rows: the rows the boundary drops still bring the
            // next sort, which can tighten it, closer.
            self.input_rows += batch.num_rows();
            let batch = self.filter_by_boundary(batch)?;
            if !batch.is_empty() {
                self.blocks.push(batch)?;
            }

            if self.input_rows >= self.batch_rows {
                let input_rows = self.input_rows;
                let output_rows = match self.flush_pending()? {
                    Some(sorted) => {
                        let rows = sorted.num_rows();
                        output.push(sorted);
                        rows
                    }
                    None => {
                        self.input_rows = 0;
                        0
                    }
                };
                self.observe_pruning(input_rows, output_rows);
            }
            data = rest;
        }
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            Ok(self.flush_pending()?.into_iter().collect())
        } else {
            self.blocks.clear();
            self.input_rows = 0;
            Ok(vec![])
        }
    }
}

/// Evaluates `$body` with `$T` set to the value type whose `compare` the sort uses for the
/// column type `$data_type`, or evaluates to `None` for the types the boundary does not
/// support.
macro_rules! with_boundary_type {
    ($data_type:expr, |$T:ident| $body:expr) => {
        match $data_type {
            DataType::Boolean => {
                type $T = BooleanType;
                $body
            }
            DataType::Binary => {
                type $T = BinaryType;
                $body
            }
            DataType::String => {
                type $T = StringType;
                $body
            }
            DataType::Date => {
                type $T = DateType;
                $body
            }
            DataType::Timestamp => {
                type $T = TimestampType;
                $body
            }
            DataType::Number(number) => with_number_mapped_type!(|NUM_TYPE| match number {
                NumberDataType::NUM_TYPE => {
                    type $T = NumberType<NUM_TYPE>;
                    $body
                }
            }),
            DataType::Decimal(size) => match size.data_kind() {
                DecimalDataKind::Decimal64 => {
                    type $T = Decimal64Type;
                    $body
                }
                DecimalDataKind::Decimal128 => {
                    type $T = Decimal128Type;
                    $body
                }
                DecimalDataKind::Decimal256 => {
                    type $T = Decimal256Type;
                    $body
                }
            },
            _ => None,
        }
    };
}

/// The rows of `entry` that are not strictly worse than `boundary` under `desc`, or `None`
/// to keep every row. Values are compared with the `compare` the sort uses for the column
/// type, and nulls are placed by `nulls_first` as the sort places them, so a row is dropped
/// only if the sort would rank it after the boundary. Rows equal to the boundary are kept:
/// their key may still rank within the limit. Other column types keep every row.
fn rows_within_boundary(
    entry: &BlockEntry,
    boundary: &Scalar,
    desc: &SortColumnDescription,
) -> Option<Bitmap> {
    let column = entry.to_column();
    let (column, validity) = split_validity(&column);
    let mut keep = with_boundary_type!(column.data_type(), |T| {
        compare_with_boundary::<T>(column, boundary, desc)
    })?;
    if let Some(validity) = validity {
        for (row, valid) in validity.iter().enumerate() {
            if !valid {
                keep.set(row, desc.nulls_first);
            }
        }
    }
    Some(keep.into())
}

fn compare_with_boundary<T: AccessType>(
    column: &Column,
    boundary: &Scalar,
    desc: &SortColumnDescription,
) -> Option<MutableBitmap> {
    let column = T::try_downcast_column(column).ok()?;
    let boundary = T::try_downcast_scalar(&boundary.as_ref()).ok()?;
    let keep = T::iter_column(&column)
        .map(|value| match T::compare(value, boundary.clone()) {
            Ordering::Less => desc.asc,
            Ordering::Greater => !desc.asc,
            Ordering::Equal => true,
        })
        .collect();
    Some(keep)
}

/// Whether the sorted `block` holds at least `n` distinct keys under `sort_desc`. A row
/// starts a new key when one of its key columns differs from the row before, compared with
/// the `compare` the sort uses for the column type; nulls equal each other. A key column of
/// another type is taken as equal on every row, which can only undercount the keys.
fn has_distinct_keys(block: &DataBlock, sort_desc: &[SortColumnDescription], n: usize) -> bool {
    let rows = block.num_rows();
    if rows == 0 {
        return false;
    }
    let mut changes = MutableBitmap::from_len_zeroed(rows);
    for desc in sort_desc {
        let column = block.get_by_offset(desc.offset).to_column();
        let (column, validity) = split_validity(&column);
        // `None` for a column type the boundary does not support, which marks nothing.
        let _ = with_boundary_type!(column.data_type(), |T| {
            mark_changes::<T>(column, validity, &mut changes)
        });
    }
    let changes = Bitmap::from(changes);
    1 + changes.len() - changes.null_count() >= n
}

/// Marks the rows of a sorted column whose value differs from the row before.
fn mark_changes<T: AccessType>(
    column: &Column,
    validity: Option<&Bitmap>,
    changes: &mut MutableBitmap,
) -> Option<()> {
    let column = T::try_downcast_column(column).ok()?;
    let mut previous: Option<Option<T::ScalarRef<'_>>> = None;
    for (row, value) in T::iter_column(&column).enumerate() {
        let value = validity
            .is_none_or(|validity| validity.get_bit(row))
            .then_some(value);
        let changed = match (&previous, &value) {
            (None, _) | (Some(None), None) => false,
            (Some(Some(previous)), Some(value)) => {
                T::compare(previous.clone(), value.clone()) != Ordering::Equal
            }
            _ => true,
        };
        if changed {
            changes.set(row, true);
        }
        previous = Some(value);
    }
    Some(())
}

/// The values of a key column and, when it is nullable, its validity.
fn split_validity(column: &Column) -> (&Column, Option<&Bitmap>) {
    match column {
        Column::Nullable(column) => (column.column(), Some(column.validity())),
        column => (column, None),
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::Decimal128Type;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::Float64Type;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;

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

    /// A transform with a boundary of its own, as the only stream of an aggregate.
    fn new_transform(sort_desc: Arc<[SortColumnDescription]>) -> TransformRankLimitSort {
        let boundary = RuntimeTopNBoundary::new(sort_desc[0].asc, sort_desc[0].nulls_first);
        TransformRankLimitSort::new(LIMIT, sort_desc, BATCH_ROWS, Arc::new(boundary))
    }

    fn transform() -> TransformRankLimitSort {
        new_transform(sort_desc())
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

    fn int(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn desc(offset: usize, asc: bool, nulls_first: bool) -> SortColumnDescription {
        SortColumnDescription {
            offset,
            asc,
            nulls_first,
        }
    }

    /// The values of the first column, nulls included.
    fn first_keys(block: &DataBlock) -> Vec<Scalar> {
        let entry = block.get_by_offset(0);
        (0..block.num_rows())
            .map(|i| entry.index(i).unwrap().to_owned())
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
        // The tail [2, 7] ranks after the boundary 1 and is dropped before the sort.
        assert!(t.blocks.block_rows().is_empty());
        assert_eq!(t.input_rows, BATCH_ROWS - 2);
        assert!(t.on_finish(true).unwrap().is_empty());
    }

    #[test]
    fn boundary_on_the_first_key_drops_rows_before_the_sort() {
        // Keys (a, b) ascending, rank limit 2.
        let sort_desc: Arc<[SortColumnDescription]> = vec![
            SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            },
            SortColumnDescription {
                offset: 1,
                asc: true,
                nulls_first: false,
            },
        ]
        .into();
        let mut t = new_transform(sort_desc);
        let pairs = |a: Vec<i32>, b: Vec<i32>| {
            DataBlock::new_from_columns(vec![Int32Type::from_data(a), Int32Type::from_data(b)])
        };

        // The second distinct key is (5, 1): the boundary is its first key, 5.
        let out = feed(
            &mut t,
            pairs(vec![9, 5, 5, 8, 7, 6, 9, 5], vec![0, 1, 2, 0, 0, 0, 0, 1]),
        );
        assert_eq!(keys(&out[0]), vec![5, 5, 5]);
        assert_eq!(t.boundary.boundary(), Some(int(5)));

        // Rows past the first key 5 are dropped; ties on 5 are kept whatever `b` is,
        // and so is the better first key 4.
        let out = feed(
            &mut t,
            pairs(vec![6, 5, 9, 4, 5, 7, 8, 6], vec![0, 9, 0, 3, 0, 0, 0, 0]),
        );
        assert_eq!(keys(&out[0]), vec![4, 5]);
        assert_eq!(t.boundary.boundary(), Some(int(5)));
        assert!(matches!(t.mode, Mode::Sort));
    }

    #[test]
    fn boundary_follows_a_descending_first_key() {
        let sort_desc: Arc<[SortColumnDescription]> = vec![SortColumnDescription {
            offset: 0,
            asc: false,
            nulls_first: false,
        }]
        .into();
        let mut t = new_transform(sort_desc);
        let out = feed(&mut t, block((0..BATCH_ROWS as i32).collect()));
        assert_eq!(keys(&out[0]), vec![7, 6]);
        // Below the boundary 6 everything is dropped; 6 and above stay.
        let out = feed(&mut t, block(vec![1, 6, 2, 9, 3, 4, 5, 0]));
        assert_eq!(keys(&out[0]), vec![9, 6]);
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
        let filter = Arc::new(RuntimeTopNBoundary::new(true, false));
        let mut t = TransformRankLimitSort::new(LIMIT, sort_desc(), BATCH_ROWS, filter.clone());

        // One distinct key is fewer than `LIMIT`, so there is no boundary, and the sort
        // switches to bypass because nothing was pruned.
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
        assert_eq!(filter.boundary(), Some(int(11)));
    }

    #[test]
    fn batch_with_exactly_limit_keys_publishes_its_last_key() {
        // The rank limit keeps the whole batch, but it holds `LIMIT` distinct keys, so
        // its last row has the `LIMIT`-th one.
        let mut t = transform();
        let out = feed(&mut t, block(vec![3, 9, 3, 9, 3, 9, 3, 9]));
        assert_eq!(out[0].num_rows(), BATCH_ROWS);
        assert_eq!(t.boundary.boundary(), Some(int(9)));
    }

    #[test]
    fn one_key_per_batch_publishes_with_limit_one() {
        // `ORDER BY k LIMIT 1` over data clustered by `k`: every batch has one key, which
        // the scans can already prune by.
        let boundary = Arc::new(RuntimeTopNBoundary::new(true, false));
        let mut t = TransformRankLimitSort::new(1, sort_desc(), BATCH_ROWS, boundary.clone());
        let out = feed(&mut t, block(vec![5; BATCH_ROWS]));
        assert_eq!(out[0].num_rows(), BATCH_ROWS);
        assert_eq!(boundary.boundary(), Some(int(5)));
    }

    #[test]
    fn limit_keys_sharing_a_first_key_publish_it() {
        // (5, 0) and (5, 1) are `LIMIT` keys with the same first key 5.
        let mut t = new_transform(vec![desc(0, true, false), desc(1, true, false)].into());
        let a = Int32Type::from_data(vec![5; BATCH_ROWS]);
        let b = Int32Type::from_data(vec![0, 1, 0, 1, 0, 1, 0, 1]);
        let out = feed(&mut t, DataBlock::new_from_columns(vec![a, b]));
        assert_eq!(out[0].num_rows(), BATCH_ROWS);
        assert_eq!(t.boundary.boundary(), Some(int(5)));

        // NULL counts as one key value: NULL and 3 are `LIMIT` keys.
        let mut t = new_transform(vec![desc(0, true, true)].into());
        let keys = vec![None, Some(3), None, Some(3), None, Some(3), None, Some(3)];
        let out = feed(
            &mut t,
            DataBlock::new_from_columns(vec![Int32Type::from_opt_data(keys)]),
        );
        assert_eq!(out[0].num_rows(), BATCH_ROWS);
        assert_eq!(t.boundary.boundary(), Some(int(3)));
    }

    /// Checks `rows_within_boundary` against the sort itself: for every non-null value of
    /// `column` as the boundary and every sort direction and null order, a row must be
    /// dropped exactly when a rank-1 sort of the row and the boundary keeps only the
    /// boundary, that is when the sort ranks the row strictly after the boundary.
    fn assert_boundary_matches_sort(column: Column) {
        assert_distinct_keys_match_sort(vec![column.clone()]);
        let data_type = column.data_type();
        let entry = BlockEntry::from(column.clone());
        for boundary in (0..column.len()).filter_map(|row| column.index(row)) {
            if boundary.is_null() {
                continue;
            }
            let boundary = boundary.to_owned();
            for (asc, nulls_first) in [(true, false), (true, true), (false, false), (false, true)] {
                let desc = desc(0, asc, nulls_first);
                let keep = rows_within_boundary(&entry, &boundary, &desc).unwrap();
                for row in 0..column.len() {
                    let value = column.index(row).unwrap();
                    let mut pair = DataBlockVec::default();
                    pair.push(DataBlock::new(vec![column.slice(row..row + 1).into()], 1))
                        .unwrap();
                    pair.push(DataBlock::new(
                        vec![BlockEntry::new_const_column(
                            data_type.clone(),
                            boundary.clone(),
                            1,
                        )],
                        1,
                    ))
                    .unwrap();
                    let ranked = pair
                        .sort_limit(vec![desc.clone()].into(), LimitType::LimitRank(1))
                        .unwrap();
                    let first = ranked.get_by_offset(0).index(0).unwrap();
                    let ranked_after = ranked.num_rows() == 1
                        && first == boundary.as_ref()
                        && value != boundary.as_ref();
                    assert_eq!(
                        keep.get_bit(row),
                        !ranked_after,
                        "value {value:?}, boundary {boundary:?}, {desc:?}"
                    );
                }
            }
        }
    }

    /// Checks `has_distinct_keys` against the sort itself: sorted rows hold at least `n`
    /// distinct keys exactly when the rank limit `n - 1` cuts them. The columns are not
    /// empty, so they hold at least one key.
    fn assert_distinct_keys_match_sort(columns: Vec<Column>) {
        let rows = columns[0].len();
        for (asc, nulls_first) in [(true, false), (true, true), (false, false), (false, true)] {
            let desc: Arc<[SortColumnDescription]> = (0..columns.len())
                .map(|offset| desc(offset, asc, nulls_first))
                .collect();
            let mut blocks = DataBlockVec::default();
            blocks
                .push(DataBlock::new_from_columns(columns.clone()))
                .unwrap();
            let sorted = blocks.sort_limit(desc.clone(), LimitType::None).unwrap();
            assert!(has_distinct_keys(&sorted, &desc, 1));
            for n in 2..=rows + 1 {
                let cut = blocks
                    .sort_limit(desc.clone(), LimitType::LimitRank(n - 1))
                    .unwrap()
                    .num_rows()
                    < rows;
                assert_eq!(
                    has_distinct_keys(&sorted, &desc, n),
                    cut,
                    "n {n}, {columns:?}, {desc:?}"
                );
            }
        }
    }

    #[test]
    fn boundary_comparison_matches_the_sort() {
        // Two key columns with repeated first keys and nulls in both.
        assert_distinct_keys_match_sort(vec![
            Int32Type::from_opt_data(vec![
                Some(1),
                Some(1),
                None,
                None,
                Some(1),
                Some(2),
                None,
                Some(2),
            ]),
            StringType::from_opt_data(vec![
                Some("x"),
                Some("y"),
                None,
                Some("x"),
                Some("x"),
                None,
                None,
                Some("y"),
            ]),
        ]);
        assert_boundary_matches_sort(Int32Type::from_opt_data(vec![
            Some(i32::MIN),
            Some(-1),
            Some(0),
            Some(3),
            None,
            Some(3),
            None,
            Some(i32::MAX),
        ]));
        assert_boundary_matches_sort(Float64Type::from_opt_data(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(0.0),
            Some(-0.0),
            None,
            Some(1.5),
            Some(-1.5),
        ]));
        assert_boundary_matches_sort(StringType::from_opt_data(vec![
            Some(""),
            Some("a"),
            Some("b"),
            Some("ba"),
            Some("B"),
            None,
            Some("\u{e9}"),
            None,
        ]));
        assert_boundary_matches_sort(BooleanType::from_opt_data(vec![
            Some(true),
            None,
            Some(false),
        ]));
        assert_boundary_matches_sort(Decimal128Type::from_opt_data_with_size(
            vec![Some(-12345_i128), Some(0), None, Some(100), Some(99)],
            Some(DecimalSize::new_unchecked(20, 2)),
        ));
        // A constant column, null or not.
        let nullable_int = DataType::Number(NumberDataType::Int32).wrap_nullable();
        let null_entry = BlockEntry::new_const_column(nullable_int.clone(), Scalar::Null, 3);
        let const_entry = BlockEntry::new_const_column(nullable_int, int(4), 2);
        let keep = |entry: &BlockEntry, desc: SortColumnDescription| {
            let keep = rows_within_boundary(entry, &int(3), &desc).unwrap();
            keep.iter().collect::<Vec<_>>()
        };
        assert_eq!(keep(&null_entry, desc(0, true, false)), vec![false; 3]);
        assert_eq!(keep(&null_entry, desc(0, true, true)), vec![true; 3]);
        assert_eq!(keep(&const_entry, desc(0, true, false)), vec![false; 2]);
        assert_eq!(keep(&const_entry, desc(0, false, false)), vec![true; 2]);
    }

    #[test]
    fn nulls_first_key_keeps_null_rows_under_the_boundary() {
        let mut t = new_transform(vec![desc(0, true, true)].into());
        let nullable = |keys: Vec<Option<i32>>| {
            DataBlock::new_from_columns(vec![Int32Type::from_opt_data(keys)])
        };

        // NULL ranks first, so the second distinct key is 3.
        let out = feed(
            &mut t,
            nullable(vec![
                Some(9),
                None,
                Some(4),
                Some(3),
                None,
                Some(8),
                Some(7),
                Some(6),
            ]),
        );
        assert_eq!(first_keys(&out[0]), vec![
            Scalar::Null,
            Scalar::Null,
            int(3)
        ]);
        assert_eq!(t.boundary.boundary(), Some(int(3)));

        // NULL rows rank before the boundary and are kept, as is the better key 2, which
        // becomes the new boundary.
        let out = feed(
            &mut t,
            nullable(vec![
                Some(5),
                None,
                Some(2),
                Some(4),
                Some(9),
                Some(3),
                Some(6),
                Some(7),
            ]),
        );
        assert_eq!(first_keys(&out[0]), vec![Scalar::Null, int(2)]);
        assert_eq!(t.boundary.boundary(), Some(int(2)));
    }

    #[test]
    fn null_first_key_of_the_limit_rank_key_bounds_nothing() {
        // Keys (a, b) with NULL last: the second distinct key is (NULL, 0).
        let mut t = new_transform(vec![desc(0, true, false), desc(1, true, false)].into());
        let a = Int32Type::from_opt_data(vec![
            Some(5),
            None,
            None,
            Some(5),
            None,
            Some(5),
            None,
            None,
        ]);
        let b = Int32Type::from_data(vec![0, 1, 0, 0, 2, 0, 0, 1]);
        let out = feed(&mut t, DataBlock::new_from_columns(vec![a, b]));
        assert_eq!(first_keys(&out[0]), vec![
            int(5),
            int(5),
            int(5),
            Scalar::Null,
            Scalar::Null
        ]);
        assert_eq!(t.boundary.boundary(), None);
    }

    #[test]
    fn boundary_on_a_string_first_key() {
        let mut t = new_transform(vec![desc(0, true, false), desc(1, true, false)].into());
        let rows = |a: Vec<&str>, b: Vec<i32>| {
            DataBlock::new_from_columns(vec![StringType::from_data(a), Int32Type::from_data(b)])
        };
        let strings = |values: &[&str]| -> Vec<Scalar> {
            values
                .iter()
                .map(|value| Scalar::String(value.to_string()))
                .collect()
        };

        // The second distinct key is ("b", 1): the boundary is "b".
        let out = feed(
            &mut t,
            rows(vec!["d", "b", "ba", "a", "c", "b", "e", "f"], vec![
                0, 2, 0, 0, 0, 1, 0, 0,
            ]),
        );
        assert_eq!(first_keys(&out[0]), strings(&["a", "b"]));
        assert_eq!(t.boundary.boundary(), Some(Scalar::String("b".to_string())));

        // "ba" sorts after "b" and is dropped before the sort; "" and both "b" rows stay
        // for the sort, which keeps ("", 0) and ("b", 5).
        let out = feed(
            &mut t,
            rows(vec!["ba", "b", "z", "", "bb", "c", "b", "d"], vec![
                0, 9, 0, 0, 0, 0, 5, 0,
            ]),
        );
        assert_eq!(first_keys(&out[0]), strings(&["", "b"]));
    }

    #[test]
    fn boundary_of_another_stream_drops_rows_before_the_sort() {
        let shared = Arc::new(RuntimeTopNBoundary::new(true, false));
        shared.update(&int(3));
        let mut t = TransformRankLimitSort::new(LIMIT, sort_desc(), BATCH_ROWS, shared.clone());

        // Every row ranks after the shared boundary 3: the whole batch is dropped, which
        // counts as effective pruning.
        let out = feed(&mut t, block(vec![9, 8, 7, 6, 5, 4, 10, 11]));
        assert!(out.is_empty());
        assert!(matches!(t.mode, Mode::Sort));

        // A tighter boundary of this stream is published to the other streams.
        let out = feed(&mut t, block(vec![9, 0, 8, 1, 7, 2, 6, 3]));
        assert_eq!(keys(&out[0]), vec![0, 1]);
        assert_eq!(shared.boundary(), Some(int(1)));
    }

    #[test]
    fn nested_first_key_is_not_filtered_before_the_sort() {
        let mut t = transform();
        let tuples = |keys: Vec<i32>| {
            let column = Column::Tuple(vec![Int32Type::from_data(keys)]);
            DataBlock::new_from_columns(vec![column])
        };
        let first = feed(&mut t, tuples((0..BATCH_ROWS as i32).rev().collect()));
        assert_eq!(first[0].num_rows(), LIMIT);
        assert!(
            rows_within_boundary(first[0].get_by_offset(0), &int(0), &desc(0, true, false))
                .is_none()
        );

        // Every row ranks after the first batch, but only the sort drops them.
        let out = feed(&mut t, tuples((10..10 + BATCH_ROWS as i32).collect()));
        assert_eq!(out[0].num_rows(), LIMIT);
    }
}
