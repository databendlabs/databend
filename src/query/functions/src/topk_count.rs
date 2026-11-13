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

//! Candidate discovery for `GROUP BY key ORDER BY COUNT(*) DESC LIMIT n`.
//!
//! The aggregate function [`TOPK_COUNT_CANDIDATES`] keeps a mergeable
//! Misra-Gries summary of the keys. Every key that the summary dropped occurs
//! at most `error` times, where `error` is the total count subtracted by the
//! reductions so far. When the summary holds at least `n` keys whose lower
//! bounds exceed `error`, the `n` most frequent keys are all among the kept
//! keys, so the function returns those kept keys that can still reach the
//! `n`-th lower bound. Otherwise it returns no candidate set.
//!
//! The scalar function [`IS_TOPK_COUNT_CANDIDATE`] keeps the rows of the
//! candidate keys, and every row when there is no candidate set. NULL keys are
//! never counted by the summary and always kept, so the NULL group is counted
//! exactly by the aggregation above the filter.

pub mod exact;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::simple_type::SimpleType;
use databend_common_expression::types::simple_type::SimpleValueType;

/// Aggregate function `topk_count_candidates(limit, capacity)(key)`.
pub const TOPK_COUNT_CANDIDATES: &str = "topk_count_candidates";
/// Scalar function `is_topk_count_candidate(candidates, key)`.
pub const IS_TOPK_COUNT_CANDIDATE: &str = "is_topk_count_candidate";

/// A key type the candidate functions can hash without allocating per row.
pub(crate) trait TopKCountKey: AccessType {
    type Key: Hash
        + Eq
        + Clone
        + Send
        + Sync
        + BorshSerialize
        + BorshDeserialize
        + Borrow<Self::Lookup>
        + 'static;
    type Lookup: ?Sized + Hash + Eq;

    fn lookup<'a>(value: &'a Self::ScalarRef<'_>) -> &'a Self::Lookup;

    fn to_key(value: Self::ScalarRef<'_>) -> Self::Key;

    fn into_scalar(
        key: Self::Key,
        data_type: &databend_common_expression::types::DataType,
    ) -> databend_common_expression::Scalar;
}

impl<T> TopKCountKey for SimpleValueType<T>
where
    T: SimpleType,
    T::Scalar: Hash + Sync + BorshSerialize + BorshDeserialize,
{
    type Key = T::Scalar;
    type Lookup = T::Scalar;

    fn lookup(value: &T::Scalar) -> &T::Scalar {
        value
    }

    fn to_key(value: T::Scalar) -> T::Scalar {
        value
    }

    fn into_scalar(
        key: Self::Key,
        data_type: &databend_common_expression::types::DataType,
    ) -> databend_common_expression::Scalar {
        T::upcast_scalar(key, data_type)
    }
}

impl TopKCountKey for StringType {
    type Key = String;
    type Lookup = str;

    fn lookup<'a>(value: &'a &str) -> &'a str {
        value
    }

    fn to_key(value: &str) -> String {
        value.to_string()
    }

    fn into_scalar(
        key: String,
        _data_type: &databend_common_expression::types::DataType,
    ) -> databend_common_expression::Scalar {
        databend_common_expression::Scalar::String(key)
    }
}

/// Calls `$body` with `$key` bound to the [`TopKCountKey`] type of a
/// non-nullable data type, or evaluates `$otherwise` for unsupported types.
///
/// Keys whose equality differs from their bytes, such as floats, are not
/// supported.
macro_rules! with_topk_count_key_type {
    ($data_type:expr, | $key:ident | $body:expr, $otherwise:expr) => {{
        use databend_common_expression::types::DataType as __DataType;
        use databend_common_expression::types::NumberDataType as __NumberDataType;
        match $data_type {
            __DataType::Number(number) => {
                databend_common_expression::with_integer_mapped_type!(|NUM| match number {
                    __NumberDataType::NUM => {
                        type $key = databend_common_expression::types::NumberType<NUM>;
                        $body
                    }
                    _ => $otherwise,
                })
            }
            __DataType::String => {
                type $key = databend_common_expression::types::StringType;
                $body
            }
            __DataType::Date => {
                type $key = databend_common_expression::types::DateType;
                $body
            }
            __DataType::Timestamp => {
                type $key = databend_common_expression::types::TimestampType;
                $body
            }
            _ => $otherwise,
        }
    }};
}

pub(crate) use with_topk_count_key_type;

/// Returns true if the candidate functions support keys of `data_type`.
pub fn is_topk_count_key_type(data_type: &databend_common_expression::types::DataType) -> bool {
    with_topk_count_key_type!(data_type.remove_nullable(), |_K| true, false)
}

/// The capacity used for a `LIMIT n` query: `max(4096, 64 * n)`.
pub fn topk_count_capacity(limit: usize) -> usize {
    limit.saturating_mul(64).max(4096)
}

/// A mergeable Misra-Gries summary.
///
/// For every key `x` with `f(x)` occurrences, `counter(x) <= f(x) <=
/// counter(x) + error`, with `counter(x) = 0` for a key the summary does not
/// hold.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HeavyHitterSummary<K: Hash + Eq> {
    capacity: usize,
    error: u64,
    counters: HashMap<K, u64, ahash::RandomState>,
}

impl<K: Hash + Eq + Clone> HeavyHitterSummary<K> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            error: 0,
            counters: HashMap::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn error(&self) -> u64 {
        self.error
    }

    #[cfg(test)]
    pub(crate) fn counter(&self, key: &K) -> u64 {
        self.counters.get(key).copied().unwrap_or(0)
    }

    pub(crate) fn add<Q>(&mut self, key: &Q, to_key: impl FnOnce() -> K)
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        if let Some(counter) = self.counters.get_mut(key) {
            *counter += 1;
            return;
        }
        self.counters.insert(to_key(), 1);
        self.maybe_reduce();
    }

    pub(crate) fn merge(&mut self, rhs: &Self) {
        self.error += rhs.error;
        for (key, count) in &rhs.counters {
            match self.counters.get_mut(key) {
                Some(counter) => *counter += *count,
                None => {
                    self.counters.insert(key.clone(), *count);
                }
            }
        }
        self.maybe_reduce();
    }

    pub(crate) fn merge_owned(&mut self, rhs: &mut Self) {
        self.error += rhs.error;
        for (key, count) in std::mem::take(&mut rhs.counters) {
            *self.counters.entry(key).or_insert(0) += count;
        }
        self.maybe_reduce();
    }

    /// Reduces lazily, at twice the capacity, so that each reduction is paid
    /// for by at least `capacity` insertions.
    fn maybe_reduce(&mut self) {
        if self.counters.len() > self.capacity.saturating_mul(2) {
            self.reduce();
        }
    }

    /// Subtracts the `(capacity + 1)`-th largest counter from every counter
    /// and drops the counters that reach zero, keeping at most `capacity`
    /// counters.
    ///
    /// Each kept counter loses exactly `pivot` and each dropped counter held at
    /// most `pivot`, so no key loses more than `pivot` and `error` remains an
    /// upper bound of what any key lost.
    fn reduce(&mut self) {
        if self.counters.len() <= self.capacity {
            return;
        }
        let mut counts = self.counters.values().copied().collect::<Vec<_>>();
        let (_, pivot, _) = counts.select_nth_unstable_by(self.capacity, |a, b| b.cmp(a));
        let pivot = *pivot;
        self.error += pivot;
        self.counters.retain(|_, count| {
            if *count > pivot {
                *count -= pivot;
                true
            } else {
                false
            }
        });
    }

    /// Returns the keys that may be among the `limit` most frequent keys, or
    /// `None` if the summary cannot prove that they include all of them.
    ///
    /// If at least `limit` counters exceed `error`, let `bound` be the
    /// `limit`-th largest counter. At least `limit` keys occur at least `bound`
    /// times, while a key the summary dropped occurs at most `error < bound`
    /// times, and a kept key with `counter + error < bound` occurs fewer than
    /// `bound` times. Neither can rank among the first `limit` keys. Without
    /// any reduction, `error` is zero and the summary holds every key.
    pub(crate) fn candidates(&self, limit: usize) -> Option<Vec<K>> {
        if limit == 0 {
            return Some(vec![]);
        }
        let bound = if self.counters.len() < limit {
            if self.error > 0 {
                return None;
            }
            0
        } else {
            let mut counts = self.counters.values().copied().collect::<Vec<_>>();
            let (_, bound, _) = counts.select_nth_unstable_by(limit - 1, |a, b| b.cmp(a));
            if *bound <= self.error {
                return None;
            }
            *bound
        };
        Some(
            self.counters
                .iter()
                .filter(|(_, count)| *count + self.error >= bound)
                .map(|(key, _)| key.clone())
                .collect(),
        )
    }
}

impl<K: Hash + Eq + BorshSerialize> BorshSerialize for HeavyHitterSummary<K> {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        (self.capacity as u64).serialize(writer)?;
        self.error.serialize(writer)?;
        (self.counters.len() as u64).serialize(writer)?;
        for (key, count) in &self.counters {
            key.serialize(writer)?;
            count.serialize(writer)?;
        }
        Ok(())
    }
}

impl<K: Hash + Eq + BorshDeserialize> BorshDeserialize for HeavyHitterSummary<K> {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let capacity = u64::deserialize_reader(reader)? as usize;
        let error = u64::deserialize_reader(reader)?;
        let len = u64::deserialize_reader(reader)? as usize;
        let mut counters = HashMap::with_capacity_and_hasher(
            len.min(capacity.saturating_mul(2) + 1),
            ahash::RandomState::default(),
        );
        for _ in 0..len {
            let key = K::deserialize_reader(reader)?;
            let count = u64::deserialize_reader(reader)?;
            counters.insert(key, count);
        }
        Ok(Self {
            capacity: capacity.max(1),
            error,
            counters,
        })
    }
}

/// Encodes the result of [`HeavyHitterSummary::candidates`].
pub(crate) fn encode_candidates<K: BorshSerialize>(
    candidates: &Option<Vec<K>>,
    writer: &mut impl Write,
) -> std::io::Result<()> {
    candidates.serialize(writer)
}

/// Decodes the output of [`encode_candidates`].
pub(crate) fn decode_candidates<K: BorshDeserialize>(
    mut data: &[u8],
) -> std::io::Result<Option<Vec<K>>> {
    Option::<Vec<K>>::deserialize_reader(&mut data)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    fn summarize(keys: &[u64], capacity: usize) -> HeavyHitterSummary<u64> {
        let mut summary = HeavyHitterSummary::new(capacity);
        for key in keys {
            summary.add(key, || *key);
        }
        summary
    }

    fn frequencies(keys: &[u64]) -> HashMap<u64, u64> {
        let mut frequencies = HashMap::new();
        for key in keys {
            *frequencies.entry(*key).or_insert(0) += 1;
        }
        frequencies
    }

    /// Checks the summary bounds of every key and, if there is a candidate
    /// set, that it holds every key occurring at least as often as the
    /// `limit`-th most frequent key.
    fn check(summary: &HeavyHitterSummary<u64>, keys: &[u64], limit: usize) -> bool {
        let frequencies = frequencies(keys);
        for (key, frequency) in &frequencies {
            let counter = summary.counter(key);
            assert!(counter <= *frequency, "key {key}: {counter} > {frequency}");
            assert!(
                *frequency <= counter + summary.error(),
                "key {key}: {frequency} > {counter} + {}",
                summary.error()
            );
        }
        let Some(candidates) = summary.candidates(limit) else {
            return false;
        };
        let candidates = candidates.into_iter().collect::<HashSet<_>>();
        let mut sorted = frequencies.values().copied().collect::<Vec<_>>();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        let bound = sorted.get(limit - 1).copied().unwrap_or(0);
        for (key, frequency) in &frequencies {
            if *frequency >= bound {
                assert!(candidates.contains(key), "missing key {key} ({frequency})");
            }
        }
        true
    }

    #[test]
    fn test_exact_without_reduction() {
        let keys = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4];
        let summary = summarize(&keys, 8);
        assert_eq!(summary.error(), 0);
        let mut candidates = summary.candidates(2).unwrap();
        candidates.sort_unstable();
        assert_eq!(candidates, vec![3, 4]);
        // Fewer keys than the limit: all of them.
        let mut candidates = summary.candidates(10).unwrap();
        candidates.sort_unstable();
        assert_eq!(candidates, vec![1, 2, 3, 4]);
        assert_eq!(summary.candidates(0), Some(vec![]));
    }

    #[test]
    fn test_ties_at_the_limit_are_kept() {
        let keys = [1, 1, 2, 2, 3, 3, 4];
        let summary = summarize(&keys, 8);
        let mut candidates = summary.candidates(1).unwrap();
        candidates.sort_unstable();
        assert_eq!(candidates, vec![1, 2, 3]);
    }

    #[test]
    fn test_uniform_keys_fall_back() {
        let keys = (0..10_000).collect::<Vec<_>>();
        let summary = summarize(&keys, 16);
        assert!(summary.error() > 0);
        assert_eq!(summary.candidates(10), None);
    }

    #[test]
    fn test_late_heavy_hitter() {
        // The heavy hitter only appears after the summary was reduced many times.
        let mut keys = (0..5_000).collect::<Vec<_>>();
        keys.extend(std::iter::repeat_n(u64::MAX, 2_000));
        let summary = summarize(&keys, 64);
        assert!(check(&summary, &keys, 1));
        assert_eq!(summary.candidates(1), Some(vec![u64::MAX]));
    }

    /// The global top key ranks below other keys on every worker.
    #[test]
    fn test_merge_finds_key_that_is_not_local_top() {
        const SHARED: u64 = u64::MAX;
        let workers = (0..3u64)
            .map(|worker| {
                let mut keys = vec![SHARED; 100];
                for local in 0..100 {
                    keys.extend(std::iter::repeat_n(worker * 1_000 + local, 101));
                }
                keys
            })
            .collect::<Vec<_>>();

        let mut merged = HeavyHitterSummary::new(64);
        for keys in &workers {
            let local = summarize(keys, 64);
            // Locally the shared key ranks last.
            assert_eq!(local.candidates(1).unwrap().len(), 100);
            assert!(!local.candidates(1).unwrap().contains(&SHARED));
            merged.merge(&local);
        }
        let keys = workers.concat();
        assert!(check(&merged, &keys, 1));
        assert!(merged.candidates(1).unwrap().contains(&SHARED));
    }

    #[test]
    fn test_random_streams_and_merges() {
        let mut rng = StdRng::seed_from_u64(0x70bc);
        let mut complete = 0;
        for case in 0..200 {
            let distinct = rng.gen_range(1..2_000u64);
            let skew = rng.gen_range(0.0..3.0f64);
            let len = rng.gen_range(0..20_000usize);
            let keys = (0..len)
                .map(|_| {
                    // Larger skew concentrates the stream on small keys.
                    let x: f64 = rng.r#gen();
                    (x.powf(1.0 + skew) * distinct as f64) as u64
                })
                .collect::<Vec<_>>();
            let capacity = rng.gen_range(1..256usize);
            let limit = rng.gen_range(1..=capacity);

            // Summarize random partitions and merge them in random order,
            // through both merge paths and serialization.
            let parts = rng.gen_range(1..8usize);
            let mut summaries = keys
                .chunks(len / parts + 1)
                .map(|chunk| summarize(chunk, capacity))
                .collect::<Vec<_>>();
            while summaries.len() > 1 {
                let lhs = summaries.swap_remove(rng.gen_range(0..summaries.len()));
                let mut rhs = summaries.swap_remove(rng.gen_range(0..summaries.len()));
                let mut lhs =
                    HeavyHitterSummary::<u64>::try_from_slice(&borsh::to_vec(&lhs).unwrap())
                        .unwrap();
                if case % 2 == 0 {
                    lhs.merge(&rhs);
                } else {
                    lhs.merge_owned(&mut rhs);
                }
                summaries.push(lhs);
            }
            let summary = summaries
                .pop()
                .unwrap_or_else(|| HeavyHitterSummary::new(capacity));
            if check(&summary, &keys, limit) {
                complete += 1;
            }
        }
        // Both outcomes are exercised.
        assert!(complete > 0 && complete < 200, "{complete} complete cases");
    }

    #[test]
    fn test_encode_candidates() {
        for candidates in [
            None,
            Some(vec![]),
            Some(vec!["a".to_string(), "b".to_string()]),
        ] {
            let mut data = vec![];
            encode_candidates(&candidates, &mut data).unwrap();
            assert_eq!(decode_candidates::<String>(&data).unwrap(), candidates);
        }
    }
}
