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

use super::Rows;

/// Linear detection budget before switching to the last-row check and galloping.
///
/// Small batches (the common case when streams interleave densely) stop within this
/// budget and never pay for the extra comparisons below.
const MAX_LINEAR_DETECTION: usize = 16;

/// Count how many rows of the sorted range `rows[start..limit)` are `<= *item`,
/// given that `rows[start] <= *item` already holds.
///
/// The returned value is in `1..=limit - start`.
///
/// This mirrors ClickHouse `SortingQueueBatch::updateBatchSize` /
/// `updateBatchSizeSlow` (`src/Core/SortCursor.h`):
///
/// 1. Linear detection for at most [`MAX_LINEAR_DETECTION`] rows.
/// 2. Last-row check: if even the last row is extractable, the whole remainder is one
///    batch. Non-intersecting key ranges cost O(1) comparisons here.
/// 3. Galloping: bracket the end of the batch with exponentially growing steps. This
///    costs O(log batch) comparisons on rows near the current position instead of
///    O(log rows_left) comparisons on far rows that miss caches.
/// 4. Binary search for the exact end inside the bracket.
///
/// Because the range is sorted, the predicate `rows[i] <= *item` is monotone, so the
/// result equals `rows[start..limit].partition_point(|r| r <= *item)`.
///
/// `item` shares the lifetime `'a` with `rows` because `Rows::Item` is a GAT:
/// `rows.row(i)` yields `Item<'a>`, and `Ord` needs both sides to be the same type.
#[inline]
pub(crate) fn batch_size_le<'a, R: Rows>(
    rows: &'a R,
    start: usize,
    limit: usize,
    item: &R::Item<'a>,
) -> usize {
    debug_assert!(start < limit);
    debug_assert!(limit <= rows.len());
    debug_assert!(rows.row(start) <= *item);

    let rows_left = limit - start;
    let ok = |offset: usize| rows.row(start + offset) <= *item;

    // updateBatchSize: `batch_size = 1`; only enter the slow path if the second row is
    // extractable too.
    if rows_left == 1 || !ok(1) {
        return 1;
    }

    batch_size_le_slow(rows_left, ok)
}

#[inline(never)]
fn batch_size_le_slow(rows_left: usize, ok: impl Fn(usize) -> bool) -> usize {
    // Rows 0 and 1 are known to be extractable.
    let mut batch_size = 2;

    // Linear detection at most 16 elements to quickly find a small batch size.
    let mut i = 0;
    while i < MAX_LINEAR_DETECTION && batch_size < rows_left && ok(batch_size) {
        batch_size += 1;
        i += 1;
    }

    if i < MAX_LINEAR_DETECTION || batch_size == rows_left {
        return batch_size;
    }

    // The batch is larger than the linear detection limit. Check the last row: if even
    // it is extractable, the whole remainder forms one batch.
    if ok(rows_left - 1) {
        return rows_left;
    }

    // Galloping detection. The last row is already known to be not extractable, so it
    // bounds the bracket.
    let mut start_offset = batch_size;
    let mut end_offset = rows_left - 1;
    let mut step = MAX_LINEAR_DETECTION;
    while start_offset + step < end_offset {
        if !ok(start_offset + step) {
            end_offset = start_offset + step;
            break;
        }
        start_offset += step + 1;
        step *= 2;
    }

    // Binary search for the exact end of the batch inside the bracketed range.
    while start_offset < end_offset {
        let mid_offset = start_offset + (end_offset - start_offset) / 2;
        if ok(mid_offset) {
            start_offset = mid_offset + 1;
        } else {
            end_offset = mid_offset;
        }
    }
    start_offset
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::super::Rows;
    use super::super::SimpleRowsAsc;
    use super::super::SimpleRowsDesc;
    use super::MAX_LINEAR_DETECTION;
    use super::batch_size_le;

    /// The original linear scan. Kept here as the primary oracle.
    fn linear_reference<'a, R: Rows>(
        rows: &'a R,
        start: usize,
        limit: usize,
        item: &R::Item<'a>,
    ) -> usize {
        let mut i = start + 1;
        while i < limit && rows.row(i) <= *item {
            i += 1;
        }
        i - start
    }

    /// Standard-library partition point. Kept here as the secondary oracle.
    fn partition_point_reference<'a, R: Rows>(
        rows: &'a R,
        start: usize,
        limit: usize,
        item: &R::Item<'a>,
    ) -> usize {
        let items: Vec<_> = (start..limit).map(|i| rows.row(i)).collect();
        items.partition_point(|r| *r <= *item)
    }

    fn asc_rows(values: &[i32]) -> SimpleRowsAsc<Int32Type> {
        SimpleRowsAsc::from_column(&Int32Type::from_data(values.to_vec())).unwrap()
    }

    fn desc_rows(values: &[i32]) -> SimpleRowsDesc<Int32Type> {
        SimpleRowsDesc::from_column(&Int32Type::from_data(values.to_vec())).unwrap()
    }

    fn check<'a, R: Rows>(rows: &'a R, start: usize, limit: usize, item: &R::Item<'a>, ctx: &str) {
        let got = batch_size_le(rows, start, limit, item);
        let lin = linear_reference(rows, start, limit, item);
        let pp = partition_point_reference(rows, start, limit, item);
        assert_eq!(got, lin, "vs linear: {ctx}");
        assert_eq!(got, pp, "vs partition_point: {ctx}");
        assert!(got >= 1 && got <= limit - start, "range: {ctx}");
    }

    /// Build `rows[start..start+n)` with exactly `run` leading rows equal to `low`
    /// (hence `<= item`) and the remainder strictly greater, under three duplicate
    /// patterns:
    ///   0: item == low, rest == low + 1        (item equals the whole run)
    ///   1: item == low, rest == low + 1..      (strictly increasing tail)
    ///   2: item between low and rest           (item matches no row)
    fn build_case(start: usize, n: usize, run: usize, pattern: u8) -> (Vec<i32>, i32) {
        let low = 100i32;
        let mut v = Vec::with_capacity(start + n);
        // Prefix before `start` is irrelevant but must keep the column sorted overall for
        // realism; use values <= low.
        v.extend(std::iter::repeat_n(low - 1, start));
        v.extend(std::iter::repeat_n(low, run));
        match pattern {
            0 => v.extend(std::iter::repeat_n(low + 1, n - run)),
            1 => v.extend((0..(n - run) as i32).map(|k| low + 1 + k)),
            _ => v.extend(std::iter::repeat_n(low + 10, n - run)),
        }
        let item = match pattern {
            0 | 1 => low,
            _ => low + 5,
        };
        (v, item)
    }

    /// Sizes chosen to hit every phase boundary:
    /// - 1, 2: entry fast paths
    /// - 17, 18, 19: linear budget edge (batch counts 2..=18 within 16 steps)
    /// - 33..36: first galloping probe at offset 18+16 = 34
    /// - 66..68: second probe at 35+32 = 67
    /// - 130..132: third probe at 68+64 = 132
    /// - 258..260: fourth probe at 133+128 = 261 (clamped by end_offset)
    /// - 1000, 8192: realistic block sizes
    const SIZES: &[usize] = &[
        1, 2, 3, 15, 16, 17, 18, 19, 20, 32, 33, 34, 35, 36, 37, 50, 51, 65, 66, 67, 68, 69, 100,
        129, 130, 131, 132, 133, 134, 257, 258, 259, 260, 261, 262, 1000, 8192,
    ];

    #[test]
    fn exhaustive_small_sizes() {
        for &start in &[0usize, 7] {
            for &n in SIZES {
                let runs: Vec<usize> = if n <= 300 {
                    (1..=n).collect()
                } else {
                    // Sample galloping boundaries ±2 plus ends for large n.
                    let mut r = vec![1, 2, 17, 18, 19, n - 1, n];
                    let mut off = 18usize;
                    let mut step = MAX_LINEAR_DETECTION;
                    while off + step < n {
                        let probe = off + step;
                        for d in probe.saturating_sub(2)..=(probe + 2).min(n) {
                            if d >= 1 {
                                r.push(d);
                            }
                        }
                        off += step + 1;
                        step *= 2;
                    }
                    r.sort_unstable();
                    r.dedup();
                    r
                };
                for run in runs {
                    for pattern in 0u8..3 {
                        let (v, item) = build_case(start, n, run, pattern);
                        let rows = asc_rows(&v);
                        let limit = start + n;
                        check(
                            &rows,
                            start,
                            limit,
                            &item,
                            &format!("start={start} n={n} run={run} pattern={pattern}"),
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn limit_shorter_than_block() {
        // `limit` may be smaller than `rows.len()` (output budget). The result must be
        // clamped to the limit, never read past it.
        let v: Vec<i32> = std::iter::repeat_n(5, 500).collect();
        let rows = asc_rows(&v);
        for limit in 1..=500usize {
            check(&rows, 0, limit, &5, &format!("all-equal limit={limit}"));
        }
        for start in 0..100usize {
            for limit in (start + 1)..=(start + 60).min(500) {
                check(
                    &rows,
                    start,
                    limit,
                    &5,
                    &format!("all-equal start={start} limit={limit}"),
                );
            }
        }
    }

    #[test]
    fn desc_order_through_reverse() {
        // Desc rows wrap items in `Reverse`, so `<=` means "value >=".
        let v: Vec<i32> = (0..300).rev().collect(); // 299, 298, ..., 0 (desc-sorted)
        let rows = desc_rows(&v);
        for run in 1..=300usize {
            // item such that exactly `run` leading rows satisfy rows[i] <= item,
            // i.e. value >= 300 - run.
            let item = std::cmp::Reverse(300 - run as i32);
            check(&rows, 0, 300, &item, &format!("desc run={run}"));
        }
    }

    #[test]
    fn random_fuzz() {
        let mut rng = StdRng::seed_from_u64(0x5eed_ba7c);
        for iter in 0..20_000 {
            let n = rng.gen_range(1..=5000usize);
            let start = rng.gen_range(0..=n.min(20));
            let total = start + n;
            // Narrow value range to produce long runs of equal values.
            let range = rng.gen_range(1..=40i32);
            let mut v: Vec<i32> = (0..total).map(|_| rng.gen_range(0..range)).collect();
            v.sort_unstable();
            let rows = asc_rows(&v);
            let limit = start + rng.gen_range(1..=n);
            // Pick item so that at least rows[start] <= item (precondition).
            let item = rng.gen_range(v[start]..=range);
            check(
                &rows,
                start,
                limit,
                &item,
                &format!("fuzz iter={iter} n={n} start={start} limit={limit} range={range}"),
            );
        }
    }
}
