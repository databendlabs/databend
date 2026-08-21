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

use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::VirtualColumnLayout;
use databend_storages_common_table_meta::meta::VirtualColumnPath;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;

const DEFAULT_MAX_DIRECT_PATHS_PER_SOURCE: usize = 1024;
const MAX_DIRECT_PATHS_HARD_LIMIT: usize = 10000;

#[derive(Clone, Copy, Debug)]
pub struct VirtualColumnLayoutPolicy {
    pub max_direct_paths_per_source: usize,
    pub max_path_statistics_per_source: usize,
}

impl Default for VirtualColumnLayoutPolicy {
    fn default() -> Self {
        Self {
            max_direct_paths_per_source: DEFAULT_MAX_DIRECT_PATHS_PER_SOURCE,
            max_path_statistics_per_source: 10000,
        }
    }
}

/// Incrementally builds one deterministic layout from all segments that
/// contribute blocks to a rewrite task.
pub struct VirtualColumnLayoutPlanner {
    policy: VirtualColumnLayoutPolicy,
    counts: HashMap<VirtualColumnPath, u64>,
    total_rows: u64,
    has_statistics: bool,
    complete: bool,
}

impl VirtualColumnLayoutPlanner {
    pub fn create(policy: VirtualColumnLayoutPolicy) -> Self {
        Self {
            policy,
            counts: HashMap::new(),
            total_rows: 0,
            has_statistics: false,
            complete: true,
        }
    }

    /// Adds blocks that share one segment-local virtual schema.
    pub fn add_blocks<'a>(
        &mut self,
        virtual_schema: Option<&VirtualSegmentSchema>,
        blocks: impl IntoIterator<Item = &'a BlockMeta>,
    ) {
        let Some(virtual_schema) = virtual_schema else {
            self.complete = false;
            return;
        };
        self.complete &= virtual_schema.is_path_statistics_complete();
        for block in blocks {
            self.total_rows += block.row_count;
            let Some(virtual_meta) = &block.virtual_block_meta else {
                self.complete = false;
                continue;
            };
            if virtual_meta.path_statistics.is_empty() && virtual_schema.path_count() > 0 {
                self.complete = false;
                continue;
            }
            self.has_statistics |= !virtual_meta.path_statistics.is_empty();
            for source in &virtual_meta.path_statistics {
                for stat in &source.paths {
                    let Some(path) = virtual_schema.path(source.source_column_id, stat.path_index)
                    else {
                        self.complete = false;
                        continue;
                    };
                    let path = VirtualColumnPath {
                        source_column_id: source.source_column_id,
                        path: path.path.clone(),
                    };
                    *self.counts.entry(path).or_default() += stat.value_count;
                }
            }
        }
    }

    pub fn build(self) -> Option<VirtualColumnLayout> {
        if self.total_rows == 0 || !self.has_statistics || !self.complete {
            return None;
        }

        let mut by_source = HashMap::<u32, Vec<(VirtualColumnPath, u64)>>::new();
        for (path, count) in self.counts {
            by_source
                .entry(path.source_column_id)
                .or_default()
                .push((path, count));
        }

        let max_direct_paths = if self.policy.max_direct_paths_per_source == 0 {
            MAX_DIRECT_PATHS_HARD_LIMIT
        } else {
            self.policy
                .max_direct_paths_per_source
                .min(MAX_DIRECT_PATHS_HARD_LIMIT)
        };
        let mut direct_paths = Vec::new();
        for paths in by_source.values_mut() {
            paths.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            direct_paths.extend(
                paths
                    .drain(..paths.len().min(max_direct_paths))
                    .map(|(path, _)| path),
            );
        }
        direct_paths.sort();
        direct_paths.dedup();
        // An empty planned layout would force every path into shared columns.
        // Fall back to per-block classification instead of locking that in.
        if direct_paths.is_empty() {
            return None;
        }
        Some(VirtualColumnLayout { direct_paths })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(source_column_id: u32, name: &str) -> VirtualColumnPath {
        VirtualColumnPath {
            source_column_id,
            path: name.to_string(),
        }
    }

    fn planner_with(
        total_rows: u64,
        counts: Vec<(VirtualColumnPath, u64)>,
    ) -> VirtualColumnLayoutPlanner {
        VirtualColumnLayoutPlanner {
            policy: VirtualColumnLayoutPolicy::default(),
            counts: counts.into_iter().collect(),
            total_rows,
            has_statistics: true,
            complete: true,
        }
    }

    #[test]
    fn high_presence_small_table_extracts_direct_paths() {
        let layout = planner_with(6, vec![
            (path(1, "a"), 4),
            (path(1, "b"), 4),
            (path(1, "c"), 4),
        ])
        .build()
        .expect("observed paths should be extracted");
        assert_eq!(layout.direct_paths.len(), 3);
        assert!(layout.contains(1, "a"));
        assert!(layout.contains(1, "b"));
        assert!(layout.contains(1, "c"));
    }

    #[test]
    fn top_k_keeps_hottest_paths_per_source() {
        let layout = planner_with(1000, vec![
            (path(1, "hot"), 80),
            (path(1, "warm"), 40),
            (path(1, "rare"), 10),
        ]);
        let mut layout = layout;
        layout.policy.max_direct_paths_per_source = 2;
        let layout = layout.build().expect("top-k paths should be extracted");
        assert_eq!(layout.direct_paths.len(), 2);
        assert!(layout.contains(1, "hot"));
        assert!(layout.contains(1, "warm"));
        assert!(!layout.contains(1, "rare"));
    }
}
