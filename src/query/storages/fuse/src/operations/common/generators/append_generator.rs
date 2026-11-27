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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::common::ConflictResolveContext;
use crate::operations::common::SnapshotGenerator;
use crate::operations::common::SnapshotMerged;
use crate::statistics::TableStatsGenerator;
use crate::statistics::reducers::merge_statistics_mut;

#[derive(Clone)]
pub struct AppendGenerator {
    overwrite: bool,
    conflict_resolve_ctx: ConflictResolveContext,
}

impl AppendGenerator {
    pub fn new(overwrite: bool) -> Self {
        AppendGenerator {
            overwrite,
            conflict_resolve_ctx: ConflictResolveContext::None,
        }
    }

    pub fn is_overwrite(&self) -> bool {
        self.overwrite
    }
}

impl AppendGenerator {
    fn conflict_resolve_ctx(&self) -> Result<(&SnapshotMerged, &TableSchema)> {
        match &self.conflict_resolve_ctx {
            ConflictResolveContext::AppendOnly((ctx, schema)) => Ok((ctx, schema.as_ref())),
            _ => Err(ErrorCode::Internal(
                "conflict_resolve_ctx should only be Appendonly in AppendGenerator",
            )),
        }
    }
}

#[async_trait::async_trait]
impl SnapshotGenerator for AppendGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_conflict_resolve_context(&mut self, ctx: ConflictResolveContext) {
        self.conflict_resolve_ctx = ctx;
    }

    fn logical_change_delta(&self, previous: &Option<Arc<TableSnapshot>>) -> (u64, u64) {
        let deleted_rows = if self.overwrite {
            previous
                .as_ref()
                .map_or(0, |snapshot| snapshot.summary.row_count)
        } else {
            0
        };
        (0, deleted_rows)
    }

    fn do_generate_new_snapshot(
        &self,
        table_info: &TableInfo,
        cluster_key_info: Option<ClusterKeyInfo>,
        previous: &Option<Arc<TableSnapshot>>,
        table_meta_timestamps: TableMetaTimestamps,
        table_stats_gen: TableStatsGenerator,
    ) -> Result<TableSnapshot> {
        let table_schema = table_info.schema().as_ref().clone();
        let (snapshot_merged, expected_schema) = self.conflict_resolve_ctx()?;
        if is_column_type_modified(&table_schema, expected_schema) {
            return Err(ErrorCode::UnresolvableConflict(format!(
                "schema was changed during insert, expected:{:?}, actual:{:?}",
                expected_schema, table_schema
            )));
        }
        let mut new_segments = snapshot_merged.merged_segments.clone();
        let mut new_summary = snapshot_merged.merged_statistics.clone();

        if let Some(snapshot) = previous {
            if !self.overwrite {
                new_segments = snapshot_merged
                    .merged_segments
                    .iter()
                    .chain(snapshot.segments.iter())
                    .cloned()
                    .collect();

                // A column range is kept only if the previous summary also has one.
                // A missing range does not show that the old rows hold the column
                // default, so none is inferred; ADD COLUMN records the default instead.
                merge_statistics_mut(
                    &mut new_summary,
                    &snapshot.summary,
                    cluster_key_info.as_ref(),
                );
            }
        }

        // merge statistics will set the additional_stats_meta to none,
        // so reset additional_stats_meta here.
        let table_statistics_location = table_stats_gen.table_statistics_location();
        for (id, ndv) in table_stats_gen.column_distinct_values() {
            if let Some(stats) = new_summary.col_stats.get_mut(&id) {
                stats.distinct_of_values = Some(ndv);
            }
        }
        new_summary.additional_stats_meta = table_stats_gen.additional_stats_meta();
        let mut snapshot = TableSnapshot::try_new(
            Some(table_info.ident.seq),
            previous.clone(),
            table_schema.clone(),
            new_summary,
            new_segments,
            cluster_key_info,
            table_statistics_location,
            table_meta_timestamps,
        )?;
        if self.overwrite {
            // No previous segment survives an overwrite.
            snapshot.segment_zone_map = None;
        }
        Ok(snapshot)
    }
}

fn is_column_type_modified(schema: &TableSchema, expected_schema: &TableSchema) -> bool {
    let expected: HashMap<_, _> = expected_schema
        .fields()
        .iter()
        .map(|f| (f.column_id, &f.data_type))
        .collect();
    schema.fields().iter().any(|f| {
        expected
            .get(&f.column_id)
            .is_some_and(|ty| **ty != f.data_type)
    })
}
