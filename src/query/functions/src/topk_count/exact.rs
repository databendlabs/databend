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

//! Bounded exact-count tracking for an adaptive, single-pass Top-K COUNT.
//!
//! The Misra-Gries residual is a lower bound, not an exact count. In addition
//! to it, keep the exact count of keys admitted before the first reduction
//! and never evicted. A key admitted later may have lost occurrences, so its
//! exact count is unknown. Merging requires exact knowledge in *every* input:
//! absence is known to mean zero only in an input that never reduced.
//!
//! Results can be returned without a rescan only when the K-th known exact
//! count strictly exceeds the upper bound of every unknown key. Strictness
//! matters: unknown keys tied on COUNT may win a secondary ORDER BY key.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberType;

use super::TopKCountKey;
use super::encode_candidates;
use super::with_topk_count_key_type;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
struct Counter {
    lower: u64,
    exact: Option<u64>,
}

/// Per-worker state with O(capacity) counters, reduced lazily at twice capacity.
#[derive(Clone, Debug)]
pub struct ExactTopKCountSummary<K> {
    capacity: usize,
    error: u64,
    nulls: u64,
    counters: HashMap<K, Counter, ahash::RandomState>,
}

impl<K: Hash + Eq + Clone> ExactTopKCountSummary<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            error: 0,
            nulls: 0,
            counters: HashMap::default(),
        }
    }

    pub fn add_null(&mut self) {
        self.nulls += 1;
    }

    pub fn add<Q>(&mut self, key: &Q, to_key: impl FnOnce() -> K)
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        if let Some(counter) = self.counters.get_mut(key) {
            counter.lower += 1;
            if let Some(exact) = &mut counter.exact {
                *exact += 1;
            }
            return;
        }
        self.counters.insert(to_key(), Counter {
            lower: 1,
            exact: (self.error == 0).then_some(1),
        });
        self.maybe_reduce();
    }

    pub fn merge_owned(&mut self, rhs: Self) {
        let lhs_error = self.error;
        // Absence in a reduced partition does not prove that a key never
        // occurred there, even if another partition knows its count exactly.
        if rhs.error > 0 {
            for (key, counter) in &mut self.counters {
                if !rhs.counters.contains_key(key) {
                    counter.exact = None;
                }
            }
        }
        self.error += rhs.error;
        self.nulls += rhs.nulls;
        for (key, rhs) in rhs.counters {
            match self.counters.get_mut(&key) {
                Some(lhs) => {
                    lhs.lower += rhs.lower;
                    lhs.exact = lhs.exact.zip(rhs.exact).map(|(a, b)| a + b);
                }
                None => {
                    self.counters.insert(key, Counter {
                        lower: rhs.lower,
                        exact: if lhs_error == 0 { rhs.exact } else { None },
                    });
                }
            }
        }
        self.maybe_reduce();
    }

    fn maybe_reduce(&mut self) {
        if self.counters.len() <= self.capacity.saturating_mul(2) {
            return;
        }
        let mut counts = self.counters.values().map(|c| c.lower).collect::<Vec<_>>();
        let (_, pivot, _) = counts.select_nth_unstable_by(self.capacity, |a, b| b.cmp(a));
        let pivot = *pivot;
        self.error += pivot;
        self.counters.retain(|_, counter| {
            if counter.lower > pivot {
                counter.lower -= pivot;
                true
            } else {
                false
            }
        });
    }

    /// Once a reduced partition has too few known keys, no continuation or
    /// merge can recover their lost occurrences. NULL may still appear later.
    pub fn can_still_prove(&self, limit: usize, nullable: bool) -> bool {
        self.error == 0
            || self.counters.values().filter(|c| c.exact.is_some()).count() + usize::from(nullable)
                >= limit
    }

    /// Proves a complete candidate set using residual lower/upper bounds,
    /// even if some candidates do not have an exact count. The caller must
    /// validate their counts in a second scan; residuals are never results.
    /// NULL is counted separately and always passes the candidate filter.
    pub fn candidates(&self, limit: usize) -> Option<Vec<K>> {
        if limit == 0 {
            return Some(vec![]);
        }
        let mut lower = self.counters.values().map(|c| c.lower).collect::<Vec<_>>();
        if self.nulls > 0 {
            lower.push(self.nulls);
        }
        let bound = if lower.len() < limit {
            if self.error > 0 {
                return None;
            }
            0
        } else {
            let (_, bound, _) = lower.select_nth_unstable_by(limit - 1, |a, b| b.cmp(a));
            if *bound <= self.error {
                return None;
            }
            *bound
        };
        Some(
            self.counters
                .iter()
                .filter(|(_, counter)| counter.lower + self.error >= bound)
                .map(|(key, _)| key.clone())
                .collect(),
        )
    }

    /// Returns exact counts sufficient for the outer Top-N, or requests a
    /// fallback. All qualifying ties are returned, not just `limit` rows.
    pub fn exact_counts(&self, limit: usize) -> Option<Vec<(Option<K>, u64)>> {
        if limit == 0 {
            return Some(vec![]);
        }
        let mut known = self
            .counters
            .values()
            .filter_map(|c| c.exact)
            .collect::<Vec<_>>();
        if self.nulls > 0 {
            known.push(self.nulls);
        }
        let bound = if known.len() < limit {
            if self.error > 0 {
                return None;
            }
            0
        } else {
            let (_, bound, _) = known.select_nth_unstable_by(limit - 1, |a, b| b.cmp(a));
            if self.error >= *bound
                || self
                    .counters
                    .values()
                    .any(|c| c.exact.is_none() && c.lower + self.error >= *bound)
            {
                return None;
            }
            *bound
        };
        let mut result = self
            .counters
            .iter()
            .filter_map(|(key, counter)| {
                counter
                    .exact
                    .filter(|count| *count >= bound)
                    .map(|count| (Some(key.clone()), count))
            })
            .collect::<Vec<_>>();
        if self.nulls > 0 && self.nulls >= bound {
            result.push((None, self.nulls));
        }
        Some(result)
    }
}

/// Type-erased, typed-column kernel used by the execution layer.
pub trait ExactTopKCountState: Send {
    fn add_block(&mut self, keys: &BlockEntry) -> Result<()>;
    fn can_still_prove(&self, limit: usize, nullable: bool) -> bool;
    fn serialize(&self) -> Result<Vec<u8>>;
    fn merge_serialized(&mut self, data: &[u8]) -> Result<()>;
    /// Encodes a proven complete candidate set for is_topk_count_candidate.
    fn candidates(&self, limit: usize) -> Result<Option<Vec<u8>>>;
    /// Output columns are COUNT followed by the grouping key.
    fn result(&self, limit: usize, key_type: &DataType) -> Option<DataBlock>;
}

struct TypedState<K: TopKCountKey>(ExactTopKCountSummary<K::Key>);

/// Creates a bounded state for any supported integer, string, date or timestamp key.
pub fn create_exact_topk_count_state(
    key_type: &DataType,
    capacity: usize,
) -> Result<Box<dyn ExactTopKCountState>> {
    with_topk_count_key_type!(
        key_type.remove_nullable(),
        |K| Ok(Box::new(TypedState::<K>(ExactTopKCountSummary::new(
            capacity
        )))),
        Err(ErrorCode::BadDataValueType(format!(
            "unsupported Top-K COUNT key: {key_type}"
        )))
    )
}

impl<K: TopKCountKey> ExactTopKCountState for TypedState<K> {
    fn add_block(&mut self, keys: &BlockEntry) -> Result<()> {
        let column = keys.to_column();
        let validity = column.validity().1;
        let values = column.remove_nullable();
        if let Ok(values) = K::try_downcast_column(&values) {
            for (row, value) in K::iter_column(&values).enumerate() {
                if validity.is_some_and(|validity| !validity.get_bit(row)) {
                    self.0.add_null();
                } else {
                    self.0.add(K::lookup(&value), || K::to_key(value.clone()));
                }
            }
            Ok(())
        } else {
            Err(ErrorCode::Internal(
                "Top-K COUNT key column does not match its type",
            ))
        }
    }

    fn can_still_prove(&self, limit: usize, nullable: bool) -> bool {
        self.0.can_still_prove(limit, nullable)
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        Ok(self.0.encode()?)
    }

    fn merge_serialized(&mut self, data: &[u8]) -> Result<()> {
        self.0.merge_owned(ExactTopKCountSummary::decode(data)?);
        Ok(())
    }

    fn candidates(&self, limit: usize) -> Result<Option<Vec<u8>>> {
        let Some(keys) = self.0.candidates(limit) else {
            return Ok(None);
        };
        let mut data = vec![];
        encode_candidates(&Some(keys), &mut data)?;
        Ok(Some(data))
    }

    fn result(&self, limit: usize, key_type: &DataType) -> Option<DataBlock> {
        let counts = self.0.exact_counts(limit)?;
        let mut keys = ColumnBuilder::with_capacity(key_type, counts.len());
        let mut values = Vec::with_capacity(counts.len());
        for (key, count) in counts {
            let key = key
                .map(|key| K::into_scalar(key, &key_type.remove_nullable()))
                .unwrap_or(Scalar::Null);
            keys.push(key.as_ref());
            values.push(count);
        }
        Some(DataBlock::new_from_columns(vec![
            NumberType::<u64>::from_data(values),
            keys.build(),
        ]))
    }
}

impl<K: Hash + Eq + BorshSerialize + BorshDeserialize> ExactTopKCountSummary<K> {
    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        let mut data = Vec::new();
        (self.capacity as u64).serialize(&mut data)?;
        self.error.serialize(&mut data)?;
        self.nulls.serialize(&mut data)?;
        (self.counters.len() as u64).serialize(&mut data)?;
        for (key, counter) in &self.counters {
            key.serialize(&mut data)?;
            counter.serialize(&mut data)?;
        }
        Ok(data)
    }

    pub fn decode(mut data: &[u8]) -> std::io::Result<Self> {
        let capacity = u64::deserialize_reader(&mut data)? as usize;
        let error = u64::deserialize_reader(&mut data)?;
        let nulls = u64::deserialize_reader(&mut data)?;
        let len = u64::deserialize_reader(&mut data)? as usize;
        let mut counters = HashMap::with_capacity_and_hasher(
            len.min(capacity.saturating_mul(2) + 1),
            ahash::RandomState::default(),
        );
        for _ in 0..len {
            counters.insert(
                K::deserialize_reader(&mut data)?,
                Counter::deserialize_reader(&mut data)?,
            );
        }
        Ok(Self {
            capacity: capacity.max(1),
            error,
            nulls,
            counters,
        })
    }
}
