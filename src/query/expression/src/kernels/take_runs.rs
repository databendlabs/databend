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

use std::ops::Range;
use std::simd::cmp::SimdOrd;
use std::simd::cmp::SimdPartialEq;
use std::simd::num::SimdUint;
use std::simd::u32x16;

use databend_common_exception::Result;

use super::take::TakeVisitor;
use super::take_ranges::TakeRangeVisitor;
use crate::DataBlock;
use crate::types::DataType;

/// Average run length from which a column of variable-length or nested
/// values copies runs: one bulk append per run replaces one push per row.
const VARIABLE_MIN_RUN_LEN: usize = 4;

/// Average run length from which a column of fixed-width values or a bitmap
/// copies runs. Gathering these rows is cheap, so only long runs gain.
const FIXED_MIN_RUN_LEN: usize = 32;

/// Number of fixed-width columns (without a bitmap) whose gains from long
/// runs pay for finding the runs.
const MIN_FIXED_COLUMNS: usize = 4;

/// Indices counted between two checks of the number of runs, so that the
/// count stops early for random indices.
const COUNT_CHUNK: usize = 1024;

const LANES: usize = 16;

/// How a column gains from copying runs of consecutive rows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RunGain {
    /// Variable-length or nested values.
    Variable,
    /// A bitmap: a Boolean column or the validity of a nullable column.
    Bitmap,
    /// Fixed-width values only.
    Fixed,
    /// No gain: the values are copied row by row anyway (`String` with the
    /// optimize-size strategy), or there are no values.
    None,
}

impl RunGain {
    fn of(data_type: &DataType) -> RunGain {
        match data_type {
            DataType::Binary
            | DataType::Variant
            | DataType::Bitmap
            | DataType::Geometry
            | DataType::Geography
            | DataType::Array(_)
            | DataType::Map(_)
            | DataType::Tuple(_)
            | DataType::Vector(_) => RunGain::Variable,
            DataType::Nullable(inner) => match RunGain::of(inner) {
                RunGain::Variable => RunGain::Variable,
                _ => RunGain::Bitmap,
            },
            DataType::AggregateState(state) => RunGain::of(state.physical_type()),
            DataType::Boolean => RunGain::Bitmap,
            DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date
            | DataType::Interval
            | DataType::Opaque(_) => RunGain::Fixed,
            DataType::Null
            | DataType::EmptyArray
            | DataType::EmptyMap
            | DataType::String
            | DataType::Generic(_)
            | DataType::StageLocation => RunGain::None,
        }
    }

    fn min_run_len(self) -> usize {
        match self {
            RunGain::Variable => VARIABLE_MIN_RUN_LEN,
            RunGain::Bitmap | RunGain::Fixed => FIXED_MIN_RUN_LEN,
            RunGain::None => usize::MAX,
        }
    }
}

impl DataBlock {
    /// Takes the rows at `indices`, in their order and with the memory
    /// strategy of [`DataBlock::take_with_optimize_size`] (`String` values
    /// are copied, not shared). The result is the same as that of
    /// `take_with_optimize_size`.
    ///
    /// When the indices form runs of consecutive rows that are long enough
    /// on average for some columns to gain, the runs are found once and those
    /// columns copy them in bulk. The other columns, and all columns for
    /// random indices, gather row by row.
    pub fn take_with_optimize_size_by_runs(&self, indices: &[u32]) -> Result<DataBlock> {
        let gains = self
            .columns()
            .iter()
            .map(|entry| match entry.as_column() {
                Some(_) => RunGain::of(&entry.data_type()),
                None => RunGain::None,
            })
            .collect::<Vec<_>>();
        let has = |gain| gains.contains(&gain);
        let fixed_columns = gains.iter().filter(|gain| **gain == RunGain::Fixed).count();
        let min_run_len = if has(RunGain::Variable) {
            VARIABLE_MIN_RUN_LEN
        } else if has(RunGain::Bitmap) || fixed_columns >= MIN_FIXED_COLUMNS {
            FIXED_MIN_RUN_LEN
        } else {
            return self.take_with_optimize_size(indices);
        };

        // Every index is a row of the block, so `index + 1` does not overflow.
        if self.num_rows() > u32::MAX as usize {
            return self.take_with_optimize_size(indices);
        }
        let Some(ranges) = index_runs(indices, min_run_len) else {
            return self.take_with_optimize_size(indices);
        };

        let num_rows = indices.len();
        let avg_run_len = num_rows / ranges.len();
        let mut range_taker = TakeRangeVisitor::new(&ranges, num_rows, true);
        let mut row_taker = TakeVisitor::new(indices).with_optimize_size_enable(true);
        let entries = self
            .columns()
            .iter()
            .zip(gains)
            .map(|(entry, gain)| {
                if avg_run_len >= gain.min_run_len() {
                    range_taker.take_entry(entry)
                } else {
                    row_taker.take_entry(entry)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::new_with_meta(
            entries,
            num_rows,
            self.get_meta().cloned(),
        ))
    }
}

/// Bit `i` is set when `indices[base + i + 1]` does not follow
/// `indices[base + i]`, that is, when a run ends at `base + i`.
#[inline(always)]
fn run_ends(indices: &[u32], base: usize) -> u64 {
    let rows = u32x16::from_slice(&indices[base..base + LANES]);
    let next = u32x16::from_slice(&indices[base + 1..base + 1 + LANES]);
    (rows + u32x16::splat(1)).simd_ne(next).to_bitmask()
}

/// Splits `indices` (in any order, duplicates allowed) into runs of
/// consecutive rows, in the order of `indices`. Returns `None` when the runs
/// are shorter than `min_run_len` on average.
///
/// Every index must be below `u32::MAX`.
fn index_runs(indices: &[u32], min_run_len: usize) -> Option<Vec<Range<u32>>> {
    let max_runs = indices.len() / min_run_len;
    if max_runs == 0 {
        return None;
    }

    // `pairs` adjacent pairs of indices; the first `simd_pairs` are compared
    // `LANES` at a time.
    let pairs = indices.len() - 1;
    let simd_pairs = pairs - pairs % LANES;
    let tail_ends = || (simd_pairs..pairs).filter(|&i| indices[i] + 1 != indices[i + 1]);

    // Count the runs before building them: the count does not allocate and,
    // for random indices, stops after about `max_runs` indices.
    let one = u32x16::splat(1);
    let mut runs = 1;
    for chunk in (0..simd_pairs).step_by(COUNT_CHUNK) {
        // 1 in each lane per run end; a lane counts at most
        // `COUNT_CHUNK / LANES` ends.
        let mut ends = u32x16::splat(0);
        for base in (chunk..simd_pairs.min(chunk + COUNT_CHUNK)).step_by(LANES) {
            let rows = u32x16::from_slice(&indices[base..base + LANES]);
            let next = u32x16::from_slice(&indices[base + 1..base + 1 + LANES]);
            ends += ((rows + one) ^ next).simd_min(one);
        }
        runs += ends.reduce_sum() as usize;
        if runs > max_runs {
            return None;
        }
    }
    runs += tail_ends().count();
    if runs > max_runs {
        return None;
    }

    let mut ranges = Vec::with_capacity(runs);
    let mut run_start = indices[0];
    let mut end_run = |i: usize| {
        ranges.push(run_start..indices[i] + 1);
        run_start = indices[i + 1];
    };
    for base in (0..simd_pairs).step_by(LANES) {
        let mut ends = run_ends(indices, base);
        while ends != 0 {
            end_run(base + ends.trailing_zeros() as usize);
            ends &= ends - 1;
        }
    }
    tail_ends().for_each(&mut end_run);
    ranges.push(run_start..indices[pairs] + 1);
    debug_assert_eq!(ranges.len(), runs);
    Some(ranges)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::index_runs;

    fn reference_runs(indices: &[u32]) -> Vec<Range<u32>> {
        let mut runs: Vec<Range<u32>> = Vec::new();
        for &index in indices {
            match runs.last_mut() {
                Some(run) if run.end == index => run.end += 1,
                _ => runs.push(index..index + 1),
            }
        }
        runs
    }

    #[test]
    fn test_index_runs() {
        let mut seed = 1_u64;
        let mut next = move |bound: u32| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) % bound as u64) as u32
        };
        // Lengths around the SIMD width and the count chunk.
        for len in [1, 2, 15, 16, 17, 31, 32, 33, 1023, 1024, 1025, 1040, 3000] {
            for max_run in [1, 3, 8, 40, 5000] {
                // Runs of random lengths below `max_run`, at random rows, with
                // repeated rows and runs going down.
                let mut indices = Vec::with_capacity(len);
                while indices.len() < len {
                    let start = next(100_000);
                    let run = (next(max_run) + 1).min((len - indices.len()) as u32);
                    match next(4) {
                        0 => indices.extend(std::iter::repeat_n(start, run as usize)),
                        1 => indices.extend((start..start + run).rev()),
                        _ => indices.extend(start..start + run),
                    }
                }
                let expected = reference_runs(&indices);
                for min_run_len in [1, 4, 32] {
                    let actual = index_runs(&indices, min_run_len);
                    if expected.len() * min_run_len <= len {
                        assert_eq!(actual.as_ref(), Some(&expected), "{len} {max_run}");
                    } else {
                        assert_eq!(actual, None, "{len} {max_run} {min_run_len}");
                    }
                }
            }
        }
        assert_eq!(index_runs(&[], 1), None);
        assert_eq!(index_runs(&[7], 2), None);
        let last = index_runs(&[u32::MAX - 1], 1).unwrap();
        assert_eq!(last.len(), 1);
        assert_eq!(last[0], u32::MAX - 1..u32::MAX);
    }

    /// The selections of `filter_executor::test_selector_take_runs` with runs
    /// take the runs path, for every column kind: the test checks the results
    /// of that path only while the thresholds stay below its run lengths.
    #[test]
    fn test_selector_test_selections_take_runs() {
        use super::FIXED_MIN_RUN_LEN;
        use super::VARIABLE_MIN_RUN_LEN;

        let clustered = (0..4096_u32).filter(|k| k % 512 < 128);
        let or_selection = (3000..4096_u32)
            .chain((0..3000).filter(|k| k % 512 < 128))
            .collect::<Vec<_>>();
        const { assert!(VARIABLE_MIN_RUN_LEN <= FIXED_MIN_RUN_LEN) };
        for selection in [clustered.collect::<Vec<_>>(), or_selection] {
            assert!(index_runs(&selection, FIXED_MIN_RUN_LEN).is_some());
        }
    }
}
