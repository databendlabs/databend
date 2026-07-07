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
use std::iter::Peekable;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::Datum;
use crate::HistogramBucket;

const MAX_LEVEL_CAPACITY: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct KllSketchItem {
    value: Datum,
    ordinal: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KllWeightedItem {
    value: Datum,
    ordinal: usize,
    weight: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KllSketch {
    level_capacity: usize,
    len: usize,
    levels: Vec<Vec<KllSketchItem>>,
    min_value: Option<Datum>,
    max_value: Option<Datum>,
    compact_next_odd: bool,
}

impl KllSketch {
    pub fn new(level_capacity: usize) -> Result<Self> {
        if level_capacity < 2 {
            return Err(ErrorCode::BadArguments(format!(
                "KLL sketch level capacity must be at least 2, got {level_capacity}"
            )));
        }
        if level_capacity > MAX_LEVEL_CAPACITY {
            return Err(ErrorCode::BadArguments(format!(
                "KLL sketch level capacity must not exceed {MAX_LEVEL_CAPACITY}, got {level_capacity}"
            )));
        }

        Ok(Self {
            level_capacity,
            len: 0,
            levels: vec![Self::new_level(level_capacity)?],
            min_value: None,
            max_value: None,
            compact_next_odd: false,
        })
    }

    pub fn with_relative_error(relative_error: f64) -> Result<Self> {
        if relative_error <= 0.0 || relative_error.is_nan() {
            return Err(ErrorCode::BadArguments(format!(
                "KLL sketch relative error must be greater than zero, got {relative_error}"
            )));
        }

        Self::new(Self::capacity_for_relative_error(relative_error)?)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn insert(&mut self, value: Datum) -> Result<()> {
        self.update_bounds(&value)?;
        self.levels[0].push(KllSketchItem {
            value,
            ordinal: self.len,
        });
        self.len += 1;
        self.compact_if_needed(0)
    }

    #[cfg(test)]
    fn levels_len(&self) -> usize {
        self.levels.len()
    }

    #[cfg(test)]
    fn retained_len(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    pub fn merge(&mut self, other: KllSketch) -> Result<()> {
        if other.is_empty() {
            return Ok(());
        }
        if self.is_empty() {
            *self = other;
            return Ok(());
        }
        if self.level_capacity != other.level_capacity {
            return Err(ErrorCode::BadArguments(format!(
                "Cannot merge KLL sketches with different level capacities: {} and {}",
                self.level_capacity, other.level_capacity
            )));
        }

        let min_value = merge_bound(&self.min_value, &other.min_value, Ordering::Less)?;
        let max_value = merge_bound(&self.max_value, &other.max_value, Ordering::Greater)?;

        self.len = self
            .len
            .checked_add(other.len)
            .ok_or_else(|| ErrorCode::BadArguments("KLL sketch length overflow during merge"))?;
        if self.levels.len() < other.levels.len() {
            while self.levels.len() < other.levels.len() {
                self.levels.push(Self::new_level(self.level_capacity)?);
            }
        }
        for (level_idx, level) in other.levels.into_iter().enumerate() {
            self.levels[level_idx].extend(level);
        }
        self.min_value = min_value;
        self.max_value = max_value;

        let mut level_idx = 0;
        while level_idx < self.levels.len() {
            self.compact_if_needed(level_idx)?;
            level_idx += 1;
        }
        Ok(())
    }

    pub fn into_equal_depth_buckets(
        self,
        num_buckets: usize,
        column_ndv: Option<f64>,
    ) -> Result<Vec<HistogramBucket>> {
        let total_values = self.len as f64;
        self.into_equal_depth_bounds(num_buckets)?
            .map(|bounds| {
                let bounds = bounds?;
                let num_distinct = estimate_bucket_ndv(bounds.num_values, total_values, column_ndv);
                HistogramBucket::try_from_bounds(
                    bounds.lower,
                    bounds.upper,
                    bounds.num_values as f64,
                    num_distinct,
                )
                .map_err(ErrorCode::Internal)
            })
            .collect()
    }

    pub fn into_equal_depth_bounds(
        self,
        num_buckets: usize,
    ) -> Result<impl Iterator<Item = Result<KllBucketBounds>>> {
        if num_buckets == 0 {
            return Err(ErrorCode::BadArguments(
                "KLL histogram bucket count must be greater than zero",
            ));
        }

        let len = self.len;
        let num_buckets = num_buckets.min(len);
        let ranks = (0..num_buckets)
            .flat_map(move |idx| [idx * len / num_buckets, ((idx + 1) * len / num_buckets) - 1]);
        let values = self.values_at_ranks(ranks);

        Ok(KllBucketBoundsIter {
            len,
            num_buckets,
            index: 0,
            values,
        })
    }

    pub fn values_at_ranks<I>(self, ranks: I) -> impl Iterator<Item = Option<Datum>>
    where I: IntoIterator<Item = usize> {
        let Self {
            level_capacity: _,
            len,
            levels,
            min_value,
            max_value,
            compact_next_odd: _,
        } = self;
        let items = collect_weighted_items(levels);

        KllValuesAtRanks {
            len,
            ranks: ranks.into_iter().peekable(),
            items: items.into_iter(),
            current: None,
            accumulated: 0,
            last_rank: None,
            min_value,
            max_value,
        }
    }

    fn compact_if_needed(&mut self, level_idx: usize) -> Result<()> {
        if self.levels[level_idx].len() <= self.level_capacity {
            return Ok(());
        }

        self.compact(level_idx)?;
        self.compact_if_needed(level_idx + 1)
    }

    fn compact(&mut self, level_idx: usize) -> Result<()> {
        let level = &mut self.levels[level_idx];
        level.sort_by(|left, right| compare_values(&left.value, &right.value).unwrap());

        let keep_odd = self.compact_next_odd;
        self.compact_next_odd = !self.compact_next_odd;

        let mut promoted = Vec::with_capacity(level.len().div_ceil(2));
        let mut retained = Vec::new();
        if level.len() % 2 == 1 {
            retained.push(level.pop().expect("odd KLL level must have a tail item"));
        }

        for (idx, item) in level.drain(..).enumerate() {
            if idx % 2 == usize::from(keep_odd) {
                promoted.push(item);
            }
        }
        *level = retained;

        if self.levels.len() == level_idx + 1 {
            self.levels.push(Self::new_level(self.level_capacity)?);
        }
        self.levels[level_idx + 1].extend(promoted);
        Ok(())
    }

    fn update_bounds(&mut self, value: &Datum) -> Result<()> {
        match &self.min_value {
            Some(min_value) => {
                if compare_values(value, min_value)?.is_lt() {
                    self.min_value = Some(value.clone());
                }
            }
            None => {
                self.min_value = Some(value.clone());
            }
        }

        match &self.max_value {
            Some(max_value) => {
                if compare_values(value, max_value)?.is_gt() {
                    self.max_value = Some(value.clone());
                }
            }
            None => {
                self.max_value = Some(value.clone());
            }
        }
        Ok(())
    }

    fn capacity_for_relative_error(relative_error: f64) -> Result<usize> {
        let capacity = (2.0 / relative_error).ceil();
        if !capacity.is_finite() || capacity > usize::MAX as f64 {
            return Err(ErrorCode::BadArguments(format!(
                "KLL sketch relative error is too small: {relative_error}"
            )));
        }
        if capacity > MAX_LEVEL_CAPACITY as f64 {
            let min_relative_error = 2.0 / MAX_LEVEL_CAPACITY as f64;
            return Err(ErrorCode::BadArguments(format!(
                "KLL sketch relative error is too small: {relative_error}, minimum supported relative error is {min_relative_error}"
            )));
        }

        Ok(capacity as usize)
    }

    fn new_level(level_capacity: usize) -> Result<Vec<KllSketchItem>> {
        let mut level = Vec::new();
        level.try_reserve_exact(level_capacity).map_err(|err| {
            ErrorCode::BadArguments(format!(
                "Cannot allocate KLL sketch level with capacity {level_capacity}: {err}"
            ))
        })?;
        Ok(level)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KllBucketBounds {
    pub lower: Datum,
    pub upper: Datum,
    pub num_values: usize,
}

struct KllBucketBoundsIter<I: Iterator<Item = Option<Datum>>> {
    len: usize,
    num_buckets: usize,
    index: usize,
    values: I,
}

impl<I: Iterator<Item = Option<Datum>>> Iterator for KllBucketBoundsIter<I> {
    type Item = Result<KllBucketBounds>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.num_buckets {
            return None;
        }

        let index = self.index;
        self.index += 1;
        let lower_rank = index * self.len / self.num_buckets;
        let upper_rank = ((index + 1) * self.len / self.num_buckets) - 1;
        let Some(lower) = self.values.next().flatten() else {
            return Some(Err(ErrorCode::Internal(
                "KLL sketch failed to produce bucket lower bound",
            )));
        };
        let Some(upper) = self.values.next().flatten() else {
            return Some(Err(ErrorCode::Internal(
                "KLL sketch failed to produce bucket upper bound",
            )));
        };

        Some(Ok(KllBucketBounds {
            lower,
            upper,
            num_values: upper_rank - lower_rank + 1,
        }))
    }
}

struct KllValuesAtRanks<I: Iterator<Item = usize>> {
    len: usize,
    ranks: Peekable<I>,
    items: std::vec::IntoIter<KllWeightedItem>,
    current: Option<KllWeightedItem>,
    accumulated: usize,
    last_rank: Option<usize>,
    min_value: Option<Datum>,
    max_value: Option<Datum>,
}

impl<I: Iterator<Item = usize>> Iterator for KllValuesAtRanks<I> {
    type Item = Option<Datum>;

    fn next(&mut self) -> Option<Self::Item> {
        let rank = self.ranks.next()?;
        debug_assert!(
            self.last_rank.is_none_or(|last_rank| rank >= last_rank),
            "KLL ranks must be sorted"
        );
        self.last_rank = Some(rank);
        if self.len == 0 {
            return Some(None);
        }

        let ordinal = rank.saturating_add(1);
        let next_ordinal = self.ranks.peek().map(|rank| rank.saturating_add(1));
        if ordinal == 1 {
            return Some(Self::emit_value(&mut self.min_value, next_ordinal, 1));
        }
        if ordinal >= self.len {
            return Some(Self::emit_value(
                &mut self.max_value,
                next_ordinal,
                usize::MAX,
            ));
        }

        while self.accumulated < ordinal {
            if let Some(item) = self.items.next() {
                self.accumulated += item.weight;
                self.current = Some(item);
            } else {
                self.current = None;
                break;
            }
        }

        Some(if self.current.is_some() && self.accumulated >= ordinal {
            let upper_ordinal = self.accumulated.min(self.len - 1);
            self.emit_current(next_ordinal, upper_ordinal)
        } else {
            None
        })
    }
}

impl<I: Iterator<Item = usize>> KllValuesAtRanks<I> {
    fn emit_value(
        value: &mut Option<Datum>,
        next_ordinal: Option<usize>,
        upper_ordinal: usize,
    ) -> Option<Datum> {
        if next_ordinal.is_some_and(|next_ordinal| next_ordinal <= upper_ordinal) {
            value.clone()
        } else {
            value.take()
        }
    }

    fn emit_current(&mut self, next_ordinal: Option<usize>, upper_ordinal: usize) -> Option<Datum> {
        if next_ordinal.is_some_and(|next_ordinal| next_ordinal <= upper_ordinal) {
            self.current.as_ref().map(|item| item.value.clone())
        } else {
            self.current.take().map(|item| item.value)
        }
    }
}

fn compare_items(left: &KllWeightedItem, right: &KllWeightedItem) -> Ordering {
    compare_values(&left.value, &right.value)
        .unwrap()
        .then(left.ordinal.cmp(&right.ordinal))
}

fn collect_weighted_items(levels: Vec<Vec<KllSketchItem>>) -> Vec<KllWeightedItem> {
    let retained_len = levels.iter().map(Vec::len).sum();
    let mut items = Vec::with_capacity(retained_len);

    for (level_idx, level) in levels.into_iter().enumerate() {
        let weight = 1usize << level_idx;
        items.extend(level.into_iter().map(|item| KllWeightedItem {
            value: item.value,
            ordinal: item.ordinal,
            weight,
        }));
    }
    items.sort_by(compare_items);
    items
}

fn compare_values(left: &Datum, right: &Datum) -> Result<Ordering> {
    left.compare(right)
}

fn merge_bound(
    left: &Option<Datum>,
    right: &Option<Datum>,
    preferred_ordering: Ordering,
) -> Result<Option<Datum>> {
    match (left, right) {
        (Some(left), Some(right)) => {
            let ordering = compare_values(left, right)?;
            let keep_left = match preferred_ordering {
                Ordering::Less => !ordering.is_gt(),
                Ordering::Equal => ordering.is_eq(),
                Ordering::Greater => !ordering.is_lt(),
            };
            Ok(Some(if keep_left { left } else { right }.clone()))
        }
        (Some(value), None) | (None, Some(value)) => Ok(Some(value.clone())),
        (None, None) => Ok(None),
    }
}

fn estimate_bucket_ndv(num_values: usize, total_values: f64, column_ndv: Option<f64>) -> f64 {
    let bucket_values = num_values as f64;
    let Some(column_ndv) = column_ndv.filter(|ndv| ndv.is_finite() && *ndv > 0.0) else {
        return bucket_values;
    };
    if total_values <= 0.0 {
        return 0.0;
    }

    (column_ndv * bucket_values / total_values).clamp(1.0, bucket_values)
}

#[cfg(test)]
mod tests {
    use super::KllSketch;
    use super::MAX_LEVEL_CAPACITY;
    use crate::Datum;

    fn build_sketch(level_capacity: usize, len: i64) -> KllSketch {
        let mut sketch = KllSketch::new(level_capacity).unwrap();
        for value in 0..len {
            sketch.insert(Datum::Int(value)).unwrap();
        }
        sketch
    }

    #[test]
    fn kll_sketch_keeps_bounded_retained_items() {
        let sketch = build_sketch(32, 10_000);

        assert_eq!(sketch.len(), 10_000);
        assert!(sketch.levels_len() > 1);
        assert!(sketch.retained_len() < 32 * 16);
    }

    #[test]
    fn kll_sketch_rejects_tiny_relative_error_before_allocation() {
        assert!(KllSketch::with_relative_error(1e-12).is_err());

        let min_relative_error = 2.0 / MAX_LEVEL_CAPACITY as f64;
        assert_eq!(
            KllSketch::capacity_for_relative_error(min_relative_error).unwrap(),
            MAX_LEVEL_CAPACITY
        );
    }

    #[test]
    fn kll_sketch_estimates_quantiles() {
        let sketch = build_sketch(256, 10_000);
        let values = sketch
            .values_at_ranks([0, 4_999, 8_999, 9_999])
            .collect::<Vec<_>>();

        assert_eq!(values[0], Some(Datum::Int(0)));
        assert_eq!(values[3], Some(Datum::Int(9999)));

        let Some(Datum::Int(p50)) = values[1] else {
            panic!("unexpected p50: {:?}", values[1]);
        };
        let Some(Datum::Int(p90)) = values[2] else {
            panic!("unexpected p90: {:?}", values[2]);
        };

        assert!((4_500..=5_500).contains(&p50), "p50={p50}");
        assert!((8_500..=9_500).contains(&p90), "p90={p90}");
    }

    #[test]
    fn kll_sketch_merges_partial_sketches() {
        let mut left = build_sketch(128, 5_000);
        let mut right = KllSketch::new(128).unwrap();
        for value in 5_000..10_000 {
            right.insert(Datum::Int(value)).unwrap();
        }
        left.merge(right).unwrap();

        let values = left.values_at_ranks([0, 4_999, 9_999]).collect::<Vec<_>>();
        assert_eq!(values[0], Some(Datum::Int(0)));
        assert_eq!(values[2], Some(Datum::Int(9999)));

        let Some(Datum::Int(p50)) = values[1] else {
            panic!("unexpected p50: {:?}", values[1]);
        };
        assert!((4_300..=5_700).contains(&p50), "p50={p50}");
    }

    #[test]
    fn kll_sketch_level_merge_keeps_bounded_items() {
        let mut merged = KllSketch::new(32).unwrap();
        let mut naive_retained_len = 0;

        for chunk in 0..200 {
            let mut sketch = KllSketch::new(32).unwrap();
            for row in 0..1_000 {
                let value = ((row * 200 + chunk) % 200_000) as i64;
                sketch.insert(Datum::Int(value)).unwrap();
            }

            naive_retained_len += sketch.retained_len();
            merged.merge(sketch).unwrap();
        }

        assert_eq!(merged.len(), 200_000);
        assert!(
            merged.retained_len() < 32 * 32,
            "retained_len={}",
            merged.retained_len()
        );
        assert!(
            merged.retained_len() * 8 < naive_retained_len,
            "retained_len={}, naive_retained_len={}",
            merged.retained_len(),
            naive_retained_len
        );

        let values = merged
            .values_at_ranks([0, 49_999, 99_999, 149_999, 199_999])
            .collect::<Vec<_>>();
        assert_eq!(values[0], Some(Datum::Int(0)));
        assert_eq!(values[4], Some(Datum::Int(199_999)));

        for (idx, (value, expected)) in values[1..4]
            .iter()
            .zip([50_000, 100_000, 150_000])
            .enumerate()
        {
            let Some(Datum::Int(value)) = value else {
                panic!(
                    "unexpected rank value at index {}: {:?}",
                    idx + 1,
                    values[idx + 1]
                );
            };
            let lower = expected - 25_000;
            let upper = expected + 25_000;
            assert!(
                (lower..=upper).contains(value),
                "expected rank around {expected}, got {value}"
            );
        }
    }

    #[test]
    fn kll_sketch_builds_equal_depth_histogram_buckets() {
        let sketch = build_sketch(256, 1_000);
        let buckets = sketch.into_equal_depth_buckets(10, Some(1_000.0)).unwrap();

        assert_eq!(buckets.len(), 10);
        assert_eq!(buckets[0].lower_bound(), Datum::Int(0));
        assert_eq!(buckets[9].upper_bound(), Datum::Int(999));
        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.num_values())
                .sum::<f64>(),
            1_000.0
        );
        assert_eq!(buckets[0].num_distinct(), 100.0);
    }

    #[test]
    fn kll_sketch_streams_equal_depth_bounds() {
        let sketch = build_sketch(32, 10_000);
        let bounds = sketch
            .into_equal_depth_bounds(10)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(bounds.len(), 10);
        assert_eq!(bounds[0].lower, Datum::Int(0));
        assert_eq!(bounds[9].upper, Datum::Int(9999));
        assert_eq!(
            bounds.iter().map(|bound| bound.num_values).sum::<usize>(),
            10_000
        );

        for bound in bounds {
            let (Datum::Int(lower), Datum::Int(upper)) = (bound.lower, bound.upper) else {
                panic!("unexpected bound type");
            };
            assert!(lower <= upper, "lower={lower}, upper={upper}");
        }
    }

    #[test]
    fn kll_sketch_streams_no_bounds_for_empty_sketch() {
        let sketch = KllSketch::new(32).unwrap();
        let bounds = sketch
            .into_equal_depth_bounds(10)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(bounds.is_empty());
    }
}
