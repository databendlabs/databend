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
use std::ops::Range;

use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub enum ConflictResolveContext {
    None,
    AppendOnly((SnapshotMerged, TableSchemaRef)),
    ModifiedSegmentExistsInLatest(SnapshotChanges),
}

impl ConflictResolveContext {
    pub fn is_latest_snapshot_append_only(
        base: &TableSnapshot,
        latest: &TableSnapshot,
    ) -> Option<Range<usize>> {
        let base_segments = &base.segments;
        let latest_segments = &latest.segments;

        let base_segments_len = base_segments.len();
        let latest_segments_len = latest_segments.len();

        if latest_segments_len >= base_segments_len
            && base_segments[0..base_segments_len]
                == latest_segments[(latest_segments_len - base_segments_len)..latest_segments_len]
        {
            Some(0..(latest_segments_len - base_segments_len))
        } else {
            None
        }
    }

    pub fn is_modified_segments_exists_in_latest(
        base: &TableSnapshot,
        latest: &TableSnapshot,
        replaced_segments: &HashMap<usize, Location>,
        removed_segments: &[usize],
    ) -> Option<(Vec<usize>, HashMap<usize, Location>)> {
        let latest_segments = latest
            .segments
            .iter()
            .enumerate()
            .map(|(i, x)| (x, i))
            .collect::<HashMap<_, usize>>();
        let mut removed = Vec::with_capacity(removed_segments.len());
        for removed_segment in removed_segments {
            let removed_segment = &base.segments[*removed_segment];
            if let Some(position) = latest_segments.get(removed_segment) {
                removed.push(*position);
            } else {
                return None;
            }
        }

        let mut replaced = HashMap::with_capacity(replaced_segments.len());
        for (position, location) in replaced_segments {
            let origin_segment = &base.segments[*position];
            if let Some(position) = latest_segments.get(origin_segment) {
                replaced.insert(*position, location.clone());
            } else {
                return None;
            }
        }
        Some((removed, replaced))
    }

    pub fn merge_segments(
        mut base_segments: Vec<Location>,
        appended_segments: Vec<Location>,
        replaced_segments: HashMap<usize, Location>,
        removed_segment_indexes: Vec<usize>,
    ) -> Vec<Location> {
        replaced_segments
            .into_iter()
            .for_each(|(k, v)| base_segments[k] = v);

        let mut blanks = removed_segment_indexes;
        blanks.sort_unstable();
        let mut merged_segments =
            Vec::with_capacity(base_segments.len() + appended_segments.len() - blanks.len());
        if !blanks.is_empty() {
            let mut last = 0;
            for blank in blanks {
                merged_segments.extend_from_slice(&base_segments[last..blank]);
                last = blank + 1;
            }
            merged_segments.extend_from_slice(&base_segments[last..]);
        } else {
            merged_segments = base_segments;
        }

        appended_segments
            .into_iter()
            .chain(merged_segments)
            .collect()
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Default)]
pub struct SnapshotChanges {
    pub appended_segments: Vec<Location>,
    pub replaced_segments: HashMap<usize, Location>,
    pub removed_segment_indexes: Vec<usize>,

    pub merged_statistics: Statistics,
    pub removed_statistics: Statistics,
}

impl SnapshotChanges {
    pub fn check_intersect(&self, other: &SnapshotChanges) -> bool {
        if Self::is_slice_intersect(&self.appended_segments, &other.appended_segments) {
            return true;
        }
        for o in &other.replaced_segments {
            if self.replaced_segments.contains_key(o.0) {
                return true;
            }
        }
        if Self::is_slice_intersect(
            &self.removed_segment_indexes,
            &other.removed_segment_indexes,
        ) {
            return true;
        }
        false
    }

    fn is_slice_intersect<T: Eq + std::hash::Hash>(l: &[T], r: &[T]) -> bool {
        let (l, r) = if l.len() > r.len() { (l, r) } else { (r, l) };
        let l = l.iter().collect::<HashSet<_>>();
        for x in r {
            if l.contains(x) {
                return true;
            }
        }
        false
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct SnapshotMerged {
    pub merged_segments: Vec<Location>,
    pub merged_statistics: Statistics,
}
