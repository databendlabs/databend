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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableField;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use log::info;
use opendal::Operator;

use super::TableMutationAggregator;
use super::new_serialize_segment_processor;
use crate::FuseTable;
use crate::io::MetaReaders;
use crate::io::MetaWriter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::SnapshotHintWriter;
use crate::operations::common::AppendGenerator;
use crate::operations::common::CommitSink;
use crate::statistics::TableStatsGenerator;

impl FuseTable {
    #[async_backtrace::framed]
    pub fn do_commit(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        prev_snapshot_id: Option<SnapshotId>,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let block_thresholds = self.get_block_thresholds();

        pipeline.try_resize(1)?;

        pipeline.add_transform(|input, output| {
            new_serialize_segment_processor(
                input,
                output,
                self,
                block_thresholds,
                table_meta_timestamps,
            )
        })?;

        pipeline.add_async_accumulating_transformer(|| {
            TableMutationAggregator::create(
                self,
                ctx.clone(),
                vec![],
                vec![],
                vec![],
                Statistics::default(),
                MutationKind::Insert,
                table_meta_timestamps,
            )
        });

        let snapshot_gen = AppendGenerator::new(ctx.clone(), overwrite);
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                copied_files.clone(),
                update_stream_meta.clone(),
                snapshot_gen.clone(),
                input,
                None,
                prev_snapshot_id,
                deduplicated_label.clone(),
                table_meta_timestamps,
            )
        })?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub async fn commit_to_meta_server(
        &self,
        ctx: &dyn TableContext,
        table_info: &TableInfo,
        location_generator: &TableMetaLocationGenerator,
        snapshot: TableSnapshot,
        table_statistics: Option<TableSnapshotStatistics>,
        copied_files: &Option<UpsertTableCopiedFileReq>,
        operator: &Operator,
    ) -> Result<()> {
        let snapshot_location = location_generator
            .gen_snapshot_location(&snapshot.snapshot_id, TableSnapshot::VERSION)?;
        let need_to_save_statistics =
            snapshot.table_statistics_location.is_some() && table_statistics.is_some();

        // 1. write down snapshot
        snapshot.write_meta(operator, &snapshot_location).await?;
        if need_to_save_statistics {
            table_statistics
                .clone()
                .unwrap()
                .write_meta(
                    operator,
                    &snapshot.table_statistics_location.clone().unwrap(),
                )
                .await?;
        }

        let table_statistics_location = snapshot.table_statistics_location();
        let catalog = ctx.get_catalog(table_info.catalog()).await?;
        // 2. update table meta
        let res = self
            .update_table_meta(
                ctx,
                catalog,
                table_info,
                location_generator,
                snapshot,
                snapshot_location,
                copied_files,
                &[],
                operator,
                None,
            )
            .await;

        if need_to_save_statistics {
            let table_statistics_location = table_statistics_location.unwrap();
            match &res {
                Ok(_) => {
                    TableSnapshotStatistics::cache()
                        .insert(table_statistics_location, table_statistics.unwrap());
                }
                Err(e) => info!("update_table_meta failed. {}", e),
            }
        }

        res
    }

    pub fn build_new_table_meta(
        old_meta: &TableMeta,
        new_snapshot_location: &str,
        new_snapshot: &TableSnapshot,
    ) -> TableMeta {
        let mut new_table_meta = old_meta.clone();
        // 1.1 set new snapshot location
        new_table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
            new_snapshot_location.to_owned(),
        );
        // remove legacy options
        Self::remove_legacy_options(&mut new_table_meta.options);

        // 1.2 setup table statistics
        let stats = &new_snapshot.summary;
        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: stats.row_count,
            data_bytes: stats.uncompressed_byte_size,
            compressed_data_bytes: stats.compressed_byte_size,
            index_data_bytes: stats.index_size,
            bloom_index_size: stats.bloom_index_size,
            ngram_index_size: stats.ngram_index_size,
            inverted_index_size: stats.inverted_index_size,
            vector_index_size: stats.vector_index_size,
            virtual_column_size: stats.virtual_column_size,
            number_of_segments: Some(new_snapshot.segments.len() as u64),
            number_of_blocks: Some(stats.block_count),
        };
        new_table_meta.updated_on = Utc::now();
        new_table_meta
    }

    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub async fn update_table_meta(
        &self,
        ctx: &dyn TableContext,
        catalog: Arc<dyn Catalog>,
        table_info: &TableInfo,
        location_generator: &TableMetaLocationGenerator,
        snapshot: TableSnapshot,
        snapshot_location: String,
        copied_files: &Option<UpsertTableCopiedFileReq>,
        update_stream_meta: &[UpdateStreamMetaReq],
        operator: &Operator,
        deduplicated_label: Option<String>,
    ) -> Result<()> {
        // 1. prepare table meta
        let new_table_meta =
            Self::build_new_table_meta(&table_info.meta, &snapshot_location, &snapshot);
        // 2. prepare the request
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let mut update_temp_tables = vec![];
        let mut update_table_metas = vec![];
        let mut copied_files_req = vec![];
        if new_table_meta.options.contains_key(OPT_KEY_TEMP_PREFIX) {
            let req = UpdateTempTableReq {
                table_id,
                new_table_meta: new_table_meta.clone(),
                copied_files: copied_files
                    .as_ref()
                    .map(|c| c.file_info.clone())
                    .unwrap_or_default(),
                desc: table_info.desc.clone(),
            };
            update_temp_tables.push(req);
        } else {
            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta: new_table_meta.clone(),
                base_snapshot_location: self.snapshot_loc(),
                lvt_check: None,
            };
            update_table_metas.push((req, table_info.clone()));
            copied_files_req = copied_files.iter().map(|c| (table_id, c.clone())).collect();
        }

        // 3. let's roll
        catalog
            .update_multi_table_meta(UpdateMultiTableMetaReq {
                update_table_metas,
                update_stream_metas: update_stream_meta.to_vec(),
                copied_files: copied_files_req,
                deduplicated_labels: deduplicated_label.into_iter().collect(),
                update_temp_tables,
            })
            .await?;

        // update_table_meta succeed, populate the snapshot cache item and try keeping a hit file of last snapshot
        TableSnapshot::cache().insert(snapshot_location.clone(), snapshot);
        Self::write_last_snapshot_hint(
            ctx,
            operator,
            location_generator,
            &snapshot_location,
            &new_table_meta,
        )
        .await;

        Ok(())
    }

    // Left a hint file which indicates the location of the latest snapshot
    #[async_backtrace::framed]
    pub async fn write_last_snapshot_hint(
        ctx: &dyn TableContext,
        operator: &Operator,
        location_generator: &TableMetaLocationGenerator,
        last_snapshot_path: &str,
        new_table_meta: &TableMeta,
    ) {
        SnapshotHintWriter::new(ctx, operator)
            .write_last_snapshot_hint(location_generator, last_snapshot_path, new_table_meta)
            .await
    }

    // check if there are any fuse table legacy options
    fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
        table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    }

    pub(crate) async fn generate_table_stats(
        &self,
        snapshot: &Option<Arc<TableSnapshot>>,
        statistics_hll: &BlockHLL,
        statistics_rows: u64,
        insert_top_n: &BlockTopN,
        refresh_top_n: bool,
    ) -> Result<TableStatsGenerator> {
        // Extract previous stats meta (row_count / hll, etc.) from snapshot.
        let summary = snapshot.summary();
        let prev_stats_meta = summary.additional_stats_meta.as_ref();
        // Previous statistics file location (if any).
        let mut prev_stats_location = snapshot.table_statistics_location();
        let top_n_columns = if refresh_top_n && statistics_rows > 0 {
            self.append_top_n_columns(self.schema())?
                .map(|(columns, _)| {
                    columns
                        .into_values()
                        .map(|field| (field.column_id(), field))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            HashMap::new()
        };
        let has_top_n_update = !top_n_columns.is_empty();
        let table_row_count = summary.row_count.saturating_add(statistics_rows);

        // If no rows are covered by commit-local statistics, just reuse previous statistics.
        if statistics_rows == 0 || (statistics_hll.is_empty() && !has_top_n_update) {
            return Ok(TableStatsGenerator::new(
                prev_stats_meta.cloned(),
                prev_stats_location,
                0,
                0,
                HashMap::new(),
                None,
            ));
        }

        // Initialize a new HLL with commit-local HLL.
        let mut new_hll = statistics_hll.clone();
        let mut next_stats_meta = if statistics_hll.is_empty() {
            prev_stats_meta.cloned()
        } else {
            None
        };
        // Calculate updated row_count
        let (hll_row_count, unstats_rows) = match (!statistics_hll.is_empty(), prev_stats_meta) {
            (false, _) => (0, 0),
            // Case 1: Previous stats exist and already contain HLL → merge directly
            (true, Some(v)) if v.hll.is_some() => {
                let prev_hll = decode_column_hll(v.hll.as_ref().unwrap())?.unwrap();
                merge_column_hll_mut(&mut new_hll, &prev_hll);
                (v.row_count + statistics_rows, v.unstats_rows)
            }
            // Case 2: Previous meta has no HLL → need to load from stats file
            (true, _) => {
                if let Some(loc) = &prev_stats_location {
                    let ver = TableMetaLocationGenerator::table_statistics_version(loc);
                    let reader = MetaReaders::table_snapshot_statistics_reader(self.get_operator());
                    let load_params = LoadParams {
                        location: loc.clone(),
                        len_hint: None,
                        ver,
                        put_cache: true,
                    };
                    let prev_stats = reader.read(&load_params).await?;
                    if prev_stats.row_count == 0 {
                        // Fallback to snapshot for real row count
                        let snapshot_loc = self.meta_location_generator().gen_snapshot_location(
                            &prev_stats.snapshot_id,
                            TableSnapshot::VERSION,
                        )?;
                        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
                        let prev_snapshot = FuseTable::read_table_snapshot_with_reader(
                            reader,
                            Some(snapshot_loc),
                            TableSnapshot::VERSION,
                        )
                        .await
                        .ok()
                        .flatten();
                        if let Some(prev) = prev_snapshot {
                            // Successfully loaded the previous snapshot.
                            merge_column_hll_mut(&mut new_hll, &prev_stats.hll);
                            let prev_rows = prev.summary.row_count;
                            (
                                prev_rows + statistics_rows,
                                summary.row_count.saturating_sub(prev_rows),
                            )
                        } else {
                            // Could not load previous snapshot → old stats are invalid
                            // Drop prev_stats_location to mark stats as "reset",
                            // and only use commit-local statistics rows as the new base.
                            prev_stats_location = None;
                            (statistics_rows, summary.row_count)
                        }
                    } else {
                        // Normal case: accumulate old row_count + commit-local statistics rows.
                        merge_column_hll_mut(&mut new_hll, &prev_stats.hll);
                        (
                            prev_stats.row_count + statistics_rows,
                            summary.row_count.saturating_sub(prev_stats.row_count),
                        )
                    }
                } else {
                    // No previous stats available.
                    (statistics_rows, summary.row_count)
                }
            }
        };

        let table_statistics = if has_top_n_update {
            self.build_append_top_n_statistics(
                snapshot,
                top_n_columns,
                &new_hll,
                insert_top_n,
                table_row_count,
            )
            .await?
        } else {
            None
        };
        if let Some(stats) = &table_statistics {
            prev_stats_location = Some(self.new_table_statistics_location(stats)?);
            if stats.hll.is_empty()
                && match next_stats_meta.as_ref().and_then(|meta| meta.hll.as_ref()) {
                    Some(hll) => decode_column_hll(hll)?.is_none_or(|hll| hll.is_empty()),
                    None => true,
                }
            {
                // The refreshed table-statistics file is authoritative for a TopN-only append.
                // Do not let metadata without any HLL values keep its pre-append row count.
                if let Some(meta) = &mut next_stats_meta {
                    meta.row_count = stats.row_count;
                }
            }
        }

        Ok(TableStatsGenerator::new(
            next_stats_meta,
            prev_stats_location,
            hll_row_count,
            unstats_rows,
            new_hll,
            table_statistics,
        ))
    }

    fn new_table_statistics_location(&self, stats: &TableSnapshotStatistics) -> Result<String> {
        self.meta_location_generator
            .snapshot_statistics_location_from_uuid(&SnapshotId::now_v7(), stats.format_version())
    }

    async fn build_append_top_n_statistics(
        &self,
        snapshot: &Option<Arc<TableSnapshot>>,
        top_n_columns: HashMap<ColumnId, TableField>,
        hll: &BlockHLL,
        insert_top_n: &BlockTopN,
        row_count: u64,
    ) -> Result<Option<TableSnapshotStatistics>> {
        let prev_table_stats = self
            .read_table_snapshot_statistics(snapshot.as_ref())
            .await?;
        let fresh_prev_stats = if let (Some(previous), Some(prev_stats)) =
            (snapshot.as_ref(), prev_table_stats.as_ref())
            && is_fresh_table_snapshot_top_n(previous, prev_stats)
        {
            Some((previous, prev_stats.as_ref()))
        } else {
            None
        };

        let mut top_n = fresh_prev_stats
            .map(|(_, stats)| stats.top_n.clone())
            .unwrap_or_default();
        top_n.retain(|column_id, _| top_n_columns.contains_key(column_id));

        let append_top_n = append_top_n_merge_input(
            &top_n_columns,
            snapshot.as_ref().map(|snapshot| &snapshot.summary),
            &top_n,
            insert_top_n,
        );
        merge_column_top_n_mut(&mut top_n, append_top_n)?;
        if top_n.is_empty() {
            return Ok(None);
        }

        // Histograms are allowed to remain stale across appends, as they still describe the
        // existing value distribution and are expensive to rebuild.
        let histograms = fresh_prev_stats
            .map(|(_, stats)| stats.histograms.clone())
            .unwrap_or_default();
        // Count-Min sketches are row-count aligned. Carrying an old sketch into the refreshed
        // statistics would incorrectly mark it as covering the appended rows.
        let count_min_sketch = HashMap::new();
        let stats_hll = if hll.is_empty() {
            fresh_prev_stats
                .map(|(_, stats)| stats.hll.clone())
                .unwrap_or_default()
        } else {
            hll.clone()
        };
        let stats_snapshot_id = snapshot
            .as_ref()
            .map(|snapshot| snapshot.snapshot_id)
            .unwrap_or_else(SnapshotId::nil);

        Ok(Some(TableSnapshotStatistics::new(
            stats_hll,
            top_n,
            count_min_sketch,
            histograms,
            stats_snapshot_id,
            row_count,
        )))
    }
}

fn append_top_n_merge_input(
    columns: &HashMap<ColumnId, TableField>,
    previous_summary: Option<&Statistics>,
    previous_top_n: &BlockTopN,
    insert_top_n: &BlockTopN,
) -> BlockTopN {
    let previous_summary = previous_summary.filter(|summary| summary.row_count != 0);
    let mut append_top_n = HashMap::new();

    for (&column_id, field) in columns {
        let Some(column_top_n) = insert_top_n.get(&column_id).cloned() else {
            continue;
        };
        if previous_top_n.contains_key(&column_id)
            || column_has_no_previous_values(previous_summary, column_id, field)
        {
            append_top_n.insert(column_id, column_top_n);
        }
    }

    append_top_n
}

fn column_has_no_previous_values(
    previous_summary: Option<&Statistics>,
    column_id: ColumnId,
    field: &TableField,
) -> bool {
    let Some(summary) = previous_summary else {
        return true;
    };
    match summary.col_stats.get(&column_id) {
        Some(stats) => stats.null_count == summary.row_count,
        None => {
            // A nullable column added without a default has no old column stats;
            // historical rows read as NULL and should not block append TopN.
            field.default_expr().is_none() && field.is_nullable_or_null()
        }
    }
}

pub(crate) fn is_fresh_table_snapshot_top_n(
    snapshot: &TableSnapshot,
    stats: &TableSnapshotStatistics,
) -> bool {
    stats.row_count == snapshot.summary.row_count
        && snapshot
            .prev_snapshot_id
            .as_ref()
            .is_none_or(|(snapshot_id, _)| *snapshot_id == stats.snapshot_id)
}
