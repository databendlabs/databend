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

use databend_common_catalog::plan::VirtualColumnLayout;
use databend_common_catalog::plan::VirtualColumnPath;
use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::DraftVirtualColumnPathStatistics;
use databend_storages_common_table_meta::meta::VirtualSegmentSchema;
use log::debug;

use crate::DEFAULT_VIRTUAL_COLUMN_MAX_DIRECT_COLUMNS;
use crate::DEFAULT_VIRTUAL_COLUMN_MAX_PATH_STATISTICS;
use crate::MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS;

#[derive(Clone, Copy, Debug)]
pub struct VirtualColumnLayoutPolicy {
    pub max_direct_columns: usize,
    pub max_path_statistics: usize,
}

impl Default for VirtualColumnLayoutPolicy {
    fn default() -> Self {
        Self {
            max_direct_columns: DEFAULT_VIRTUAL_COLUMN_MAX_DIRECT_COLUMNS,
            max_path_statistics: DEFAULT_VIRTUAL_COLUMN_MAX_PATH_STATISTICS,
        }
    }
}

/// Incrementally builds a deterministic best-effort layout from the bounded
/// path statistics retained by all segments in one rewrite task.
pub struct VirtualColumnLayoutPlanner {
    policy: VirtualColumnLayoutPolicy,
    /// Counts grouped by source column. A path String is allocated only on its
    /// first observation, rather than once per contributing block.
    counts: HashMap<ColumnId, HashMap<String, u64>>,
    total_rows: u64,
    statistics_complete: bool,
}

impl VirtualColumnLayoutPlanner {
    pub fn create(policy: VirtualColumnLayoutPolicy) -> Self {
        Self {
            policy,
            counts: HashMap::new(),
            total_rows: 0,
            statistics_complete: true,
        }
    }

    fn add_count(&mut self, source_column_id: ColumnId, path: &str, count: u64) {
        let source_counts = self.counts.entry(source_column_id).or_default();
        if let Some(value) = source_counts.get_mut(path) {
            *value += count;
        } else {
            source_counts.insert(path.to_owned(), count);
        }
    }

    pub fn add_draft_statistics(
        &mut self,
        statistics: &HashMap<ColumnId, DraftVirtualColumnPathStatistics>,
        row_count: u64,
    ) {
        self.total_rows += row_count;
        for (source_column_id, source) in statistics {
            self.statistics_complete &= source.path_statistics_complete;
            for (path, value_count) in &source.path_counts {
                self.add_count(*source_column_id, path, *value_count as u64);
            }
        }
    }

    /// Adds blocks that share one segment-local virtual schema. Missing or
    /// truncated metadata makes planning approximate, but retained counts remain
    /// valid heavy-hitter evidence and still participate in layout selection.
    pub fn add_blocks<'a>(
        &mut self,
        virtual_schema: Option<&VirtualSegmentSchema>,
        blocks: impl IntoIterator<Item = &'a BlockMeta>,
    ) {
        let blocks = blocks.into_iter();
        let Some(virtual_schema) = virtual_schema else {
            self.statistics_complete = false;
            self.total_rows += blocks.map(|block| block.row_count).sum::<u64>();
            return;
        };
        // Build the direct-id lookup once for all blocks sharing this schema.
        let column_id_to_path = virtual_schema
            .column_paths
            .iter()
            .flat_map(|source| {
                source.paths.iter().map(move |path| {
                    (
                        path.column_id,
                        (source.source_column_id, path.path.as_str()),
                    )
                })
            })
            .collect::<HashMap<_, _>>();

        for block in blocks {
            self.total_rows += block.row_count;

            if let Some(virtual_meta) = &block.virtual_block_meta {
                for (column_id, meta) in &virtual_meta.virtual_column_metas {
                    let Some((source_column_id, path)) = column_id_to_path.get(column_id).copied()
                    else {
                        self.statistics_complete = false;
                        continue;
                    };
                    let value_count = meta
                        .column_stat
                        .as_ref()
                        .map(|stat| meta.num_values.saturating_sub(stat.null_count))
                        .unwrap_or(meta.num_values);
                    self.add_count(source_column_id, path, value_count);
                }
            }

            // Direct paths represented by `virtual_column_metas` are not stored
            // again in `virtual_path_statistics`; these sources are disjoint.
            if let Some(statistics) = &block.virtual_path_statistics {
                for (source_column_id, source) in statistics {
                    self.statistics_complete &= source.path_statistics_complete;
                    for (column_id, value_count) in &source.path_counts {
                        let Some((path_source_column_id, path)) =
                            virtual_schema.field_of_column_id(*column_id)
                        else {
                            self.statistics_complete = false;
                            continue;
                        };
                        if path_source_column_id != *source_column_id {
                            self.statistics_complete = false;
                            continue;
                        }
                        self.add_count(*source_column_id, &path.path, *value_count as u64);
                    }
                }
            }
        }
    }

    pub fn build(self) -> Option<VirtualColumnLayout> {
        if self.total_rows == 0 || self.counts.is_empty() {
            debug!(
                "virtual column layout not planned: total_rows={}, statistics_complete={}",
                self.total_rows, self.statistics_complete
            );
            return None;
        }

        let observed_path_count = self.counts.values().map(HashMap::len).sum::<usize>();
        let max_direct_paths = if self.policy.max_direct_columns == 0 {
            MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS
        } else {
            self.policy
                .max_direct_columns
                .min(MAX_VIRTUAL_COLUMN_DIRECT_COLUMNS)
        };
        let mut direct_paths = Vec::new();
        for (source_column_id, counts) in self.counts {
            let mut paths = counts.into_iter().collect::<Vec<_>>();
            paths.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            direct_paths.extend(paths.into_iter().take(max_direct_paths).map(|(path, _)| {
                VirtualColumnPath {
                    source_column_id,
                    path,
                }
            }));
        }
        direct_paths.sort();

        if direct_paths.is_empty() {
            debug!(
                "virtual column layout not planned: no direct paths selected from {} observed paths",
                observed_path_count
            );
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
        let mut grouped = HashMap::<ColumnId, HashMap<String, u64>>::new();
        for (path, count) in counts {
            grouped
                .entry(path.source_column_id)
                .or_default()
                .insert(path.path, count);
        }
        VirtualColumnLayoutPlanner {
            policy: VirtualColumnLayoutPolicy::default(),
            counts: grouped,
            total_rows,
            statistics_complete: true,
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
        let mut planner = planner_with(1000, vec![
            (path(1, "hot"), 80),
            (path(1, "warm"), 40),
            (path(1, "rare"), 10),
        ]);
        planner.policy.max_direct_columns = 2;
        let layout = planner.build().expect("top-k paths should be extracted");
        assert_eq!(layout.direct_paths.len(), 2);
        assert!(layout.contains(1, "hot"));
        assert!(layout.contains(1, "warm"));
        assert!(!layout.contains(1, "rare"));
    }

    #[test]
    fn incomplete_retained_statistics_still_build_layout() {
        let mut planner = planner_with(100, vec![(path(1, "hot"), 80)]);
        planner.statistics_complete = false;
        let layout = planner.build().expect("retained heavy hitter is usable");
        assert!(layout.contains(1, "hot"));
    }
}
