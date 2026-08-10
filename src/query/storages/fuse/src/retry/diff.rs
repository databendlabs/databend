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
use std::collections::HashSet;

use databend_storages_common_table_meta::meta::Location;

#[derive(Clone)]
pub struct SegmentsDiff {
    appended: Vec<Location>,
    replaced: HashMap<Location, Vec<Location>>,
}

impl SegmentsDiff {
    #[allow(clippy::needless_range_loop)]
    pub fn new(base_segments: &[Location], new_segments: &[Location]) -> Self {
        // base_segments is empty
        if base_segments.is_empty() {
            return SegmentsDiff {
                appended: new_segments.to_vec(),
                replaced: HashMap::new(),
            };
        }

        let mut common_indices = Vec::new();
        let new_segment_map: HashMap<_, _> = new_segments
            .iter()
            .enumerate()
            .map(|(idx, loc)| (&loc.0, idx))
            .collect();

        for (i, base) in base_segments.iter().enumerate() {
            if let Some(&j) = new_segment_map.get(&base.0) {
                common_indices.push((i, j));
            }
        }

        let mut replaced = HashMap::new();
        let mut appended = Vec::new();
        let mut prev_base_idx = 0;
        let mut prev_new_idx = 0;

        // first common element is the first element of base_segments
        if let Some(&(base_idx, new_idx)) = common_indices.first() {
            if base_idx == 0 {
                appended.extend(new_segments[..new_idx].iter().cloned());
            }
        }

        // process the elements between common elements
        for &(base_idx, new_idx) in &common_indices {
            for i in prev_base_idx..base_idx {
                let mut replacements = Vec::new();

                if i == prev_base_idx && prev_new_idx < new_idx {
                    for j in prev_new_idx..new_idx {
                        replacements.push(new_segments[j].clone());
                    }
                }

                replaced.insert(base_segments[i].clone(), replacements);
            }

            prev_base_idx = base_idx + 1;
            prev_new_idx = new_idx + 1;
        }

        // Process the remaining elements after the last common element
        for i in prev_base_idx..base_segments.len() {
            let mut replacements = Vec::new();

            if i == prev_base_idx && prev_new_idx < new_segments.len() {
                for j in prev_new_idx..new_segments.len() {
                    replacements.push(new_segments[j].clone());
                }
            }

            replaced.insert(base_segments[i].clone(), replacements);
        }

        SegmentsDiff { replaced, appended }
    }

    pub fn apply(self, target: Vec<Location>) -> Option<Vec<Location>> {
        let target_segments = target.iter().collect::<HashSet<_>>();
        for base in self.replaced.keys() {
            if !target_segments.contains(base) {
                return None;
            }
        }

        let Self {
            appended,
            mut replaced,
        } = self;

        let mut new_segments = appended;
        for segment in target.into_iter() {
            match replaced.remove(&segment) {
                Some(replacements) => {
                    new_segments.extend(replacements);
                }
                None => {
                    new_segments.push(segment);
                }
            }
        }
        Some(new_segments)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use databend_common_expression::TableSchema;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::TableMetaTimestamps;
    use databend_storages_common_table_meta::meta::TableSnapshot;

    use super::*;

    fn snapshot_from_segments(segments: Vec<&str>) -> TableSnapshot {
        let mut snapshot = TableSnapshot::try_new(
            None,
            None,
            TableSchema::default(),
            Statistics::default(),
            vec![],
            None,
            None,
            // Dummy timestamps for test
            TableMetaTimestamps::new(None, Duration::hours(1)),
        )
        .unwrap();
        snapshot.segments = segments.iter().map(|s| (s.to_string(), 0)).collect();
        snapshot
    }

    #[test]
    fn test_segments_edition() {
        {
            let base_snapshot = snapshot_from_segments(vec!["a", "b", "c", "d", "e", "f", "g"]);
            let new_snapshot = snapshot_from_segments(vec!["x", "y", "b", "m", "n", "f", "p"]);
            let segments_edition =
                SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
            let mut replaced = segments_edition
                .replaced
                .iter()
                .map(|(l, o)| {
                    (
                        l.0.as_str(),
                        o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            replaced.sort_by_key(|(k, _)| k.to_string());
            let appended: Vec<&str> = segments_edition
                .appended
                .iter()
                .map(|l| l.0.as_str())
                .collect::<Vec<_>>();
            assert_eq!(replaced, vec![
                ("a", vec!["x", "y"]),
                ("c", vec!["m", "n"]),
                ("d", vec![]),
                ("e", vec![]),
                ("g", vec!["p"])
            ]);
            assert!(appended.is_empty());
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec!["a", "b", "c", "d", "e", "f", "g"]);
                let new_snapshot = snapshot_from_segments(vec![]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let mut replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                replaced.sort_by_key(|(k, _)| k.to_string());
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(replaced, vec![
                    ("a", vec![]),
                    ("b", vec![]),
                    ("c", vec![]),
                    ("d", vec![]),
                    ("e", vec![]),
                    ("f", vec![]),
                    ("g", vec![]),
                ]);
                assert!(appended.is_empty());
            }
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec!["a", "b", "c", "d", "e", "f", "g"]);
                let new_snapshot = snapshot_from_segments(vec!["z"]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let mut replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                replaced.sort_by_key(|(k, _)| k.to_string());
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(replaced, vec![
                    ("a", vec!["z"]),
                    ("b", vec![]),
                    ("c", vec![]),
                    ("d", vec![]),
                    ("e", vec![]),
                    ("f", vec![]),
                    ("g", vec![]),
                ]);
                assert!(appended.is_empty());
            }
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec!["a"]);
                let new_snapshot = snapshot_from_segments(vec!["x", "y", "z", "a"]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(appended, vec!["x", "y", "z"]);
                assert!(replaced.is_empty());
            }
        }

        {
            {
                let base_snapshot = snapshot_from_segments(vec![]);
                let new_snapshot = snapshot_from_segments(vec!["x", "y", "z"]);
                let segments_edition =
                    SegmentsDiff::new(&base_snapshot.segments, &new_snapshot.segments);
                let replaced = segments_edition
                    .replaced
                    .iter()
                    .map(|(l, o)| {
                        (
                            l.0.as_str(),
                            o.iter().map(|l| l.0.as_str()).collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>();
                let appended = segments_edition
                    .appended
                    .iter()
                    .map(|l| l.0.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(appended, vec!["x", "y", "z"]);
                assert!(replaced.is_empty());
            }
        }
    }
}
