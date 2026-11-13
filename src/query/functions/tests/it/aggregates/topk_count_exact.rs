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

use std::collections::HashMap;

use databend_common_functions::topk_count::exact::ExactTopKCountSummary;

fn add(summary: &mut ExactTopKCountSummary<u64>, key: Option<u64>) {
    match key {
        Some(key) => summary.add(&key, || key),
        None => summary.add_null(),
    }
}

fn summary(rows: &[Option<u64>], capacity: usize) -> ExactTopKCountSummary<u64> {
    let mut result = ExactTopKCountSummary::new(capacity);
    for &key in rows {
        add(&mut result, key);
    }
    result
}

fn check(rows: &[Option<u64>], state: &ExactTopKCountSummary<u64>, limit: usize) -> bool {
    let mut frequencies = HashMap::<Option<u64>, u64>::new();
    for &key in rows {
        *frequencies.entry(key).or_default() += 1;
    }
    let Some(result) = state.exact_counts(limit) else {
        return false;
    };
    let actual = result.into_iter().collect::<HashMap<_, _>>();
    for (key, count) in &actual {
        assert_eq!(frequencies.get(key), Some(count), "COUNT must be exact");
    }
    let mut ordered = frequencies.values().copied().collect::<Vec<_>>();
    ordered.sort_unstable_by(|a, b| b.cmp(a));
    let bound = ordered.get(limit.saturating_sub(1)).copied().unwrap_or(0);
    for (key, count) in &frequencies {
        if *count >= bound {
            assert_eq!(
                actual.get(key),
                Some(count),
                "all Top-K ties must be retained"
            );
        }
    }
    true
}

#[test]
fn exact_counts_survive_reductions_and_serialization() {
    let mut rows = vec![Some(0); 100];
    for i in 1..1000 {
        rows.extend([Some(0), Some(i), None]);
    }
    let state = summary(&rows, 8);
    let restored = ExactTopKCountSummary::decode(&state.encode().unwrap()).unwrap();
    assert!(check(&rows, &restored, 2));
    assert!(state.exact_counts(3).is_none());
}

#[test]
fn late_winner_and_unknown_ties_require_fallback() {
    let mut rows = (0..100).map(Some).collect::<Vec<_>>();
    rows.extend(std::iter::repeat_n(Some(0), 1000));
    let state = summary(&rows, 4);
    assert!(state.exact_counts(1).is_none());
    assert!(!state.can_still_prove(1, false));
    assert!(
        state.can_still_prove(1, true),
        "a NULL winner could still appear"
    );

    // A known count cannot beat an unknown key tied at its upper bound.
    let rows = [
        Some(0),
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(5),
    ];
    assert!(summary(&rows, 2).exact_counts(1).is_none());
}

#[test]
fn absent_in_a_reduced_partition_is_not_zero() {
    let lhs_rows = vec![Some(0); 1000];
    let rhs_rows = (0..100).map(Some).collect::<Vec<_>>();
    let mut lhs = summary(&lhs_rows, 4);
    lhs.merge_owned(summary(&rhs_rows, 4));
    assert!(lhs.exact_counts(1).is_none());
    let mut rhs = summary(&rhs_rows, 4);
    rhs.merge_owned(summary(&lhs_rows, 4));
    assert!(rhs.exact_counts(1).is_none());

    // An unreduced partition can certify absence; merging it must not
    // unnecessarily destroy an otherwise exact count.
    let mut lhs = summary(&lhs_rows, 4);
    lhs.merge_owned(summary(&[Some(1)], 4));
    assert_eq!(lhs.exact_counts(1), Some(vec![(Some(0), 1000)]));
}

#[test]
fn empty_nulls_and_fewer_groups_than_limit() {
    assert_eq!(summary(&[], 4).exact_counts(10), Some(vec![]));
    let rows = [None, None, Some(1), Some(1), Some(2)];
    assert!(check(&rows, &summary(&rows, 4), 10));
    assert!(check(&rows, &summary(&rows, 4), 1));
    let rows = vec![None; 100];
    assert_eq!(summary(&rows, 4).exact_counts(10), Some(vec![(None, 100)]));
}

#[test]
fn randomized_partitioned_proof_is_always_exact() {
    let mut random = 42_u64;
    let mut successes = 0;
    let mut fallbacks = 0;
    for trial in 0..200 {
        let capacity = 1 + trial % 31;
        let workers = 1 + trial % 7;
        let mut rows = vec![];
        let mut states = (0..workers)
            .map(|_| ExactTopKCountSummary::new(capacity))
            .collect::<Vec<_>>();
        let mut impossible = [false; 4];
        for i in 0..4000 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = match (random >> 32) % 10 {
                0 => None,
                1..=4 if trial % 2 == 0 => Some(0),
                _ => Some((random >> 40) % 1000),
            };
            rows.push(key);
            add(&mut states[i % workers], key);
            for (j, limit) in [1, 2, 3, 10].into_iter().enumerate() {
                impossible[j] |= !states[i % workers].can_still_prove(limit, true);
            }
        }
        let mut merged = ExactTopKCountSummary::new(capacity);
        for state in states {
            merged.merge_owned(ExactTopKCountSummary::decode(&state.encode().unwrap()).unwrap());
        }
        for (j, limit) in [1, 2, 3, 10].into_iter().enumerate() {
            if impossible[j] {
                assert!(
                    merged.exact_counts(limit).is_none(),
                    "early fallback must be irreversible"
                );
            }
            if !merged.can_still_prove(limit, true) {
                assert!(merged.exact_counts(limit).is_none());
            }
            if check(&rows, &merged, limit) {
                successes += 1;
            } else {
                fallbacks += 1;
            }
        }
    }
    assert!(successes > 0);
    assert!(fallbacks > 0);
}
