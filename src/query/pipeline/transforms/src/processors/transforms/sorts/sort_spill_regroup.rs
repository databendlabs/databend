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
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub(super) trait IntervalGroupingPayload {
    type Key<'a>: Ord
    where Self: 'a;

    fn start(&self) -> Self::Key<'_>;
    fn end(&self) -> Self::Key<'_>;
}

pub(super) fn regroup_min_interval_groups<T, U>(mut items: Vec<T>) -> Vec<Vec<U>>
where T: IntervalGroupingPayload + Into<U> {
    #[derive(Debug)]
    struct ActiveRef<'a, T: IntervalGroupingPayload> {
        payload: &'a T,
        group: usize,
    }

    impl<T: IntervalGroupingPayload> Eq for ActiveRef<'_, T> {}

    impl<T: IntervalGroupingPayload> PartialEq for ActiveRef<'_, T> {
        fn eq(&self, other: &Self) -> bool {
            self.cmp(other) == Ordering::Equal
        }
    }

    impl<T: IntervalGroupingPayload> PartialOrd for ActiveRef<'_, T> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl<T: IntervalGroupingPayload> Ord for ActiveRef<'_, T> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.payload
                .end()
                .cmp(&other.payload.end())
                .then_with(|| self.group.cmp(&other.group))
        }
    }

    if items.is_empty() {
        return Vec::new();
    }

    items.sort_by(|lhs, rhs| {
        lhs.start()
            .cmp(&rhs.start())
            .then_with(|| lhs.end().cmp(&rhs.end()))
    });

    let mut active = BinaryHeap::<Reverse<ActiveRef<'_, T>>>::new();
    let mut groups = Vec::new();

    let assignments = items
        .iter()
        .map(|payload| {
            if let Some(mut top) = active.peek_mut()
                && top.0.payload.end() <= payload.start()
            {
                top.0.payload = payload;
                top.0.group
            } else {
                let group = groups.len();
                groups.push(Vec::new());
                active.push(Reverse(ActiveRef { payload, group }));
                group
            }
        })
        .collect::<Vec<_>>();

    for (payload, i) in items.into_iter().zip(assignments) {
        groups[i].push(payload.into());
    }

    groups
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    struct TestPayload {
        start: i32,
        end: i32,
        id: i32,
    }

    impl IntervalGroupingPayload for TestPayload {
        type Key<'a>
            = i32
        where Self: 'a;

        fn start(&self) -> Self::Key<'_> {
            self.start
        }

        fn end(&self) -> Self::Key<'_> {
            self.end
        }
    }

    fn min_groups_required(input: &[(i32, i32, i32)]) -> usize {
        let mut events = Vec::with_capacity(input.len() * 2);
        for (start, end, _) in input {
            // end is processed before start at same position, because end <= start is non-overlap.
            events.push((*end, 0_i8, -1_i32));
            events.push((*start, 1_i8, 1_i32));
        }
        events.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0).then(lhs.1.cmp(&rhs.1)));

        let mut curr = 0;
        let mut peak = 0;
        for (_, _, delta) in events {
            curr += delta;
            peak = peak.max(curr);
        }
        peak as usize
    }

    #[test]
    fn test_regroup_min_interval_groups() {
        struct Case {
            name: &'static str,
            input: Vec<(i32, i32, i32)>,
        }

        let cases = vec![
            Case {
                name: "empty",
                input: vec![],
            },
            Case {
                name: "single",
                input: vec![(1, 2, 1)],
            },
            Case {
                name: "disjoint_unsorted",
                input: vec![(5, 6, 3), (1, 2, 1), (3, 4, 2)],
            },
            Case {
                name: "all_overlap",
                input: vec![(1, 5, 1), (2, 6, 2), (3, 7, 3)],
            },
            Case {
                name: "chain_overlap",
                input: vec![(1, 4, 1), (2, 6, 2), (5, 7, 3)],
            },
            Case {
                name: "same_start",
                input: vec![(1, 2, 1), (1, 3, 2), (1, 4, 3), (4, 5, 4)],
            },
            Case {
                name: "touching_boundaries",
                input: vec![(1, 2, 1), (2, 3, 2), (3, 4, 3), (0, 1, 4)],
            },
            Case {
                name: "negative_mixed",
                input: vec![(-3, -1, 1), (-2, 2, 2), (2, 3, 3), (0, 1, 4)],
            },
        ];

        for case in cases {
            let name = case.name;
            let input = case.input;
            let groups: Vec<Vec<TestPayload>> = regroup_min_interval_groups(
                input
                    .iter()
                    .map(|(start, end, id)| TestPayload {
                        start: *start,
                        end: *end,
                        id: *id,
                    })
                    .collect::<Vec<_>>(),
            );

            assert_eq!(
                groups.len(),
                min_groups_required(&input),
                "group count mismatch in case {name}"
            );

            let mut ids = BTreeSet::new();
            for group in groups {
                let mut prev_end = None;
                for payload in group {
                    assert!(
                        ids.insert(payload.id),
                        "duplicated id {} in case {name}",
                        payload.id
                    );
                    if let Some(end) = prev_end {
                        assert!(
                            end <= payload.start,
                            "overlapped interval in one group, case {name}: {end} > {}",
                            payload.start
                        );
                    }
                    prev_end = Some(payload.end);
                }
            }

            let expected_ids = input.iter().map(|(_, _, id)| *id).collect::<BTreeSet<_>>();
            assert_eq!(
                ids, expected_ids,
                "payload coverage mismatch in case {name}"
            );
        }
    }
}
