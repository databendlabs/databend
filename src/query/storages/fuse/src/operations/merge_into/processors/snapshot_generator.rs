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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchema;
use common_sql::field_default_value;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

use crate::metrics::metrics_inc_commit_mutation_unresolvable_conflict;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::deduct_statistics;
use crate::statistics::reducers::merge_statistics_mut;

pub trait SnapshotGenerator {
    fn set_merged_segments(&mut self, segments: Vec<Location>);

    fn set_merged_summary(&mut self, summary: Statistics);

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot>;
}

#[derive(Clone)]
pub struct MutationGenerator {
    base_snapshot: Arc<TableSnapshot>,
    merged_segments: Vec<Location>,
    merged_statistics: Statistics,
}

impl MutationGenerator {
    pub fn new(base_snapshot: Arc<TableSnapshot>) -> Self {
        MutationGenerator {
            base_snapshot,
            merged_segments: vec![],
            merged_statistics: Statistics::default(),
        }
    }
}

impl SnapshotGenerator for MutationGenerator {
    fn set_merged_segments(&mut self, segments: Vec<Location>) {
        self.merged_segments = segments;
    }

    fn set_merged_summary(&mut self, summary: Statistics) {
        self.merged_statistics = summary;
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot> {
        let base_segments = &self.base_snapshot.segments;
        let base_segments_len = self.base_snapshot.segments.len();
        let base_statistics = &self.base_snapshot.summary;

        if previous.is_none() {
            if base_segments_len > 0 {
                metrics_inc_commit_mutation_unresolvable_conflict();
                return Err(ErrorCode::StorageOther(
                    "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                ));
            }

            let new_snapshot = TableSnapshot::new(
                Uuid::new_v4(),
                &None,
                None,
                schema,
                self.merged_statistics.clone(),
                self.merged_segments.clone(),
                cluster_key_meta,
                None,
            );
            return Ok(new_snapshot);
        }

        let previous = previous.unwrap();
        let latest_segments = &previous.segments;
        let latest_statistics = &previous.summary;
        let latest_segments_len = latest_segments.len();

        if latest_segments_len < base_segments_len
            || base_segments[0..base_segments_len]
                != latest_segments[(latest_segments_len - base_segments_len)..latest_segments_len]
        {
            metrics_inc_commit_mutation_unresolvable_conflict();
            return Err(ErrorCode::StorageOther(
                "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
            ));
        }

        let append_segments =
            latest_segments[0..(latest_segments_len - base_segments_len)].to_owned();
        let append_statistics = deduct_statistics(latest_statistics, base_statistics);

        let new_segments = append_segments
            .iter()
            .chain(self.merged_segments.iter())
            .cloned()
            .collect::<Vec<_>>();
        let new_summary = merge_statistics(&self.merged_statistics, &append_statistics);
        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &previous.timestamp,
            Some((previous.snapshot_id, previous.format_version)),
            schema,
            new_summary,
            new_segments,
            cluster_key_meta,
            previous.table_statistics_location.clone(),
        );
        Ok(new_snapshot)
    }
}

#[derive(Clone)]
pub struct AppendGenerator {
    ctx: Arc<dyn TableContext>,
    merged_segments: Vec<Location>,
    merged_statistics: Statistics,

    overwrite: bool,
}

impl AppendGenerator {
    pub fn new(ctx: Arc<dyn TableContext>, overwrite: bool) -> Self {
        AppendGenerator {
            ctx,
            merged_segments: vec![],
            merged_statistics: Statistics::default(),
            overwrite,
        }
    }
}

impl SnapshotGenerator for AppendGenerator {
    fn set_merged_segments(&mut self, segments: Vec<Location>) {
        self.merged_segments = segments;
    }

    fn set_merged_summary(&mut self, summary: Statistics) {
        self.merged_statistics = summary;
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot> {
        let mut new_segments = self.merged_segments.clone();
        let mut new_summary = self.merged_statistics.clone();
        let new_snapshot = if let Some(snapshot) = &previous {
            if !self.overwrite {
                let mut summary = snapshot.summary.clone();
                let mut fill_default_values = false;
                // check if need to fill default value in statistics
                for column_id in self.merged_statistics.col_stats.keys() {
                    if !summary.col_stats.contains_key(column_id) {
                        fill_default_values = true;
                        break;
                    }
                }
                if fill_default_values {
                    let mut default_values = Vec::with_capacity(schema.num_fields());
                    for field in schema.fields() {
                        default_values.push(field_default_value(self.ctx.clone(), field)?);
                    }
                    let leaf_default_values = schema.field_leaf_default_values(&default_values);
                    leaf_default_values
                        .iter()
                        .for_each(|(col_id, default_value)| {
                            if !summary.col_stats.contains_key(col_id) {
                                let (null_count, distinct_of_values) = if default_value.is_null() {
                                    (summary.row_count, Some(0))
                                } else {
                                    (0, Some(1))
                                };
                                let col_stat = ColumnStatistics {
                                    min: default_value.to_owned(),
                                    max: default_value.to_owned(),
                                    null_count,
                                    in_memory_size: 0,
                                    distinct_of_values,
                                };
                                summary.col_stats.insert(*col_id, col_stat);
                            }
                        });
                }

                new_segments = self
                    .merged_segments
                    .iter()
                    .chain(snapshot.segments.iter())
                    .cloned()
                    .collect();
                merge_statistics_mut(&mut new_summary, &summary);
            };
            TableSnapshot::new(
                Uuid::new_v4(),
                &snapshot.timestamp,
                Some((snapshot.snapshot_id, snapshot.format_version)),
                schema,
                new_summary,
                new_segments,
                cluster_key_meta,
                snapshot.table_statistics_location.clone(),
            )
        } else {
            TableSnapshot::new(
                Uuid::new_v4(),
                &None,
                None,
                schema,
                new_summary,
                new_segments,
                cluster_key_meta,
                None,
            )
        };

        Ok(new_snapshot)
    }
}
