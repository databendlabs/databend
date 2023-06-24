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
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_sql::field_default_value;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

use crate::metrics::metrics_inc_commit_mutation_resolvable_conflict;
use crate::metrics::metrics_inc_commit_mutation_unresolvable_conflict;
use crate::operations::commit::Conflict;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::deduct_statistics;
use crate::statistics::reducers::merge_statistics_mut;

#[async_trait::async_trait]
pub trait SnapshotGenerator {
    fn set_merged_segments(&mut self, segments: Vec<Location>);

    fn set_merged_summary(&mut self, summary: Statistics);

    fn set_context(&mut self, ctx: ConflictResolveContext);

    async fn fill_default_values(
        &mut self,
        _schema: TableSchema,
        _snapshot: &Option<Arc<TableSnapshot>>,
    ) -> Result<()> {
        Ok(())
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot>;

    /// How to add a new method of conflict detection:
    ///
    /// 1. add a new `SnapshotGenerator`
    ///
    /// 2. design a new variant of enum `MutationConflictResolveContext`, and pass it from pipeline to `SnapshotGenerator`
    ///
    /// 3. impl method `detect_conflicts` for the new `SnapshotGenerator`
    fn detect_conflicts(&self, _lastest: &TableSnapshot) -> Result<Conflict> {
        Err(ErrorCode::Unimplemented(
            "detect_conflicts is not implemented",
        ))
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct MutationConflictResolveContext {
    pub modified_segments: Vec<Location>,
    pub modified_statistics: Statistics,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Default)]
pub enum ConflictResolveContext {
    Mutation(Box<MutationConflictResolveContext>),
    Append,
    Compact,
    #[default]
    Uninitialized,
}

#[derive(Clone)]
pub struct MutationGenerator {
    base_snapshot: Arc<TableSnapshot>,
    merged_segments: Vec<Location>,
    merged_statistics: Statistics,
    ctx: Option<Box<MutationConflictResolveContext>>,
}

impl MutationGenerator {
    pub fn new(base_snapshot: Arc<TableSnapshot>) -> Self {
        MutationGenerator {
            base_snapshot,
            merged_segments: vec![],
            merged_statistics: Statistics::default(),
            ctx: None,
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

    fn detect_conflicts(&self, latest: &TableSnapshot) -> Result<Conflict> {
        let latest_segments = &latest.segments;
        let modified_segments = &self.ctx.as_ref().unwrap().modified_segments;
        if modified_segments
            .iter()
            .all(|modified_segment| latest_segments.contains(modified_segment))
        {
            Ok(Conflict::ResolvableDataMutate)
        } else {
            Ok(Conflict::Unresolvable)
        }
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot> {
        let previous =
            previous.unwrap_or_else(|| Arc::new(TableSnapshot::new_empty_snapshot(schema.clone())));

        match self.detect_conflicts(&previous)? {
            Conflict::Unresolvable => {
                metrics_inc_commit_mutation_unresolvable_conflict();
                Err(ErrorCode::StorageOther(
                    "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                ))
            }
            Conflict::ResolvableAppend(range_of_newly_append) => {
                tracing::info!("resolvable conflicts detected");
                metrics_inc_commit_mutation_resolvable_conflict();
                let append_segments = &previous.segments[range_of_newly_append];
                let append_statistics =
                    deduct_statistics(&previous.summary, &self.base_snapshot.summary);

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
            Conflict::ResolvableDataMutate => {
                tracing::info!("resolvable conflicts detected");
                metrics_inc_commit_mutation_resolvable_conflict();
                let mut new_segments = previous.segments.clone();
                let ctx = self.ctx.as_ref().unwrap();
                new_segments.retain(|x| !ctx.modified_segments.contains(x));
                new_segments.extend(self.merged_segments.clone());
                let new_summary = merge_statistics(&self.merged_statistics, &previous.summary);
                let new_summary = deduct_statistics(&new_summary, &ctx.modified_statistics);
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
    }

    fn set_context(&mut self, ctx: ConflictResolveContext) {
        match ctx {
            ConflictResolveContext::Mutation(ctx) => {
                self.ctx = Some(ctx);
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct AppendGenerator {
    ctx: Arc<dyn TableContext>,
    merged_segments: Vec<Location>,
    merged_statistics: Statistics,
    leaf_default_values: HashMap<ColumnId, Scalar>,

    overwrite: bool,
}

impl AppendGenerator {
    pub fn new(ctx: Arc<dyn TableContext>, overwrite: bool) -> Self {
        AppendGenerator {
            ctx,
            merged_segments: vec![],
            merged_statistics: Statistics::default(),
            leaf_default_values: HashMap::new(),
            overwrite,
        }
    }

    fn check_fill_default(&self, summary: &Statistics) -> bool {
        let mut fill_default_values = false;
        // check if need to fill default value in statistics
        for column_id in self.merged_statistics.col_stats.keys() {
            if !summary.col_stats.contains_key(column_id) {
                fill_default_values = true;
                break;
            }
        }
        fill_default_values
    }
}

#[async_trait::async_trait]
impl SnapshotGenerator for AppendGenerator {
    fn set_merged_segments(&mut self, segments: Vec<Location>) {
        self.merged_segments = segments;
    }

    fn set_merged_summary(&mut self, summary: Statistics) {
        self.merged_statistics = summary;
    }

    fn set_context(&mut self, ctx: ConflictResolveContext) {
        match ctx {
            ConflictResolveContext::Append => {}
            _ => unreachable!(),
        }
    }

    async fn fill_default_values(
        &mut self,
        schema: TableSchema,
        previous: &Option<Arc<TableSnapshot>>,
    ) -> Result<()> {
        if let Some(snapshot) = previous {
            if !self.overwrite && self.check_fill_default(&snapshot.summary) {
                let mut default_values = Vec::with_capacity(schema.num_fields());
                for field in schema.fields() {
                    default_values.push(field_default_value(self.ctx.clone(), field)?);
                }
                self.leaf_default_values = schema.field_leaf_default_values(&default_values);
            }
        }
        Ok(())
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot> {
        let mut prev_timestamp = None;
        let mut prev_snapshot_id = None;
        let mut table_statistics_location = None;
        let mut new_segments = self.merged_segments.clone();
        let mut new_summary = self.merged_statistics.clone();

        if let Some(snapshot) = &previous {
            prev_timestamp = snapshot.timestamp;
            prev_snapshot_id = Some((snapshot.snapshot_id, snapshot.format_version));
            table_statistics_location = snapshot.table_statistics_location.clone();

            if !self.overwrite {
                let mut summary = snapshot.summary.clone();
                if self.check_fill_default(&summary) {
                    self.leaf_default_values
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
            }
        }

        Ok(TableSnapshot::new(
            Uuid::new_v4(),
            &prev_timestamp,
            prev_snapshot_id,
            schema,
            new_summary,
            new_segments,
            cluster_key_meta,
            table_statistics_location,
        ))
    }
}
