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

use backoff::backoff::Backoff;
use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_meta_types::MatchSeq;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use log::debug;
use log::info;
use opendal::Operator;

use super::TableMutationAggregator;
use super::decorate_snapshot;
use super::new_serialize_segment_processor;
use crate::FuseTable;
use crate::io::MetaReaders;
use crate::io::MetaWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::SnapshotHintWriter;
use crate::operations::common::AppendGenerator;
use crate::operations::common::CommitSink;
use crate::operations::common::ConflictResolveContext;
use crate::operations::set_backoff;
use crate::statistics::TableStatsGenerator;
use crate::statistics::merge_statistics;

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
                ctx.clone(),
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
        let snapshot_location = location_generator.gen_snapshot_location(
            self.get_branch_id(),
            &snapshot.snapshot_id,
            TableSnapshot::VERSION,
        )?;
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
        &self,
        old_meta: &TableMeta,
        new_snapshot_location: &str,
        new_snapshot: &TableSnapshot,
    ) -> Result<TableMeta> {
        let mut new_table_meta = old_meta.clone();
        if let Some(branch_name) = self.get_branch_name() {
            let Some(branch_ref) = new_table_meta.refs.get_mut(branch_name) else {
                return Err(ErrorCode::UnknownReference(format!(
                    "Unknown branch '{}'",
                    branch_name
                )));
            };
            branch_ref.loc = new_snapshot_location.to_owned();
        } else {
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
        }
        new_table_meta.updated_on = Utc::now();
        Ok(new_table_meta)
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
        let should_write_hint = self.branch_info.is_none();
        let new_table_meta =
            self.build_new_table_meta(&table_info.meta, &snapshot_location, &snapshot)?;
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
        if should_write_hint {
            Self::write_last_snapshot_hint(
                ctx,
                operator,
                location_generator,
                &snapshot_location,
                &new_table_meta,
            )
            .await;
        }

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

    // TODO use commit sink instead
    #[async_backtrace::framed]
    pub async fn commit_mutation(
        &self,
        ctx: &Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        base_segments: &[Location],
        base_summary: Statistics,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let mut retries = 0;
        let mut backoff = set_backoff(None, None, None);

        let mut latest_snapshot = base_snapshot.clone();
        let mut latest_table_info = &self.table_info;
        let mut latest_schema = self.schema();
        let default_cluster_key_id = self.cluster_key_id();
        let base_schema = self.schema();

        // holding the reference of latest table during retries
        let mut latest_table_ref: Arc<dyn Table>;

        // potentially concurrently appended segments, init it to empty
        let mut concurrently_appended_segment_locations: &[Location] = &[];

        // Status
        ctx.set_status_info("mutation: begin try to commit");

        loop {
            let mut snapshot_tobe_committed = TableSnapshot::try_from_previous(
                latest_snapshot.clone(),
                Some(latest_table_info.ident.seq),
                table_meta_timestamps,
            )?;

            if base_schema != latest_schema {
                return Err(ErrorCode::StorageOther(
                    "The schema of the table has changed",
                ));
            }
            let (segments_tobe_committed, statistics_tobe_committed) = Self::merge_with_base(
                ctx.clone(),
                self.operator.clone(),
                base_segments,
                &base_summary,
                concurrently_appended_segment_locations,
                latest_schema.clone(),
                default_cluster_key_id,
            )
            .await?;
            snapshot_tobe_committed.segments = segments_tobe_committed;
            snapshot_tobe_committed.summary = statistics_tobe_committed;
            snapshot_tobe_committed.summary.additional_stats_meta = latest_snapshot
                .summary
                .additional_stats_meta
                .as_ref()
                .cloned();

            decorate_snapshot(
                &mut snapshot_tobe_committed,
                ctx.txn_mgr(),
                Some(base_snapshot.clone()),
                self.get_id(),
            )?;

            match self
                .commit_to_meta_server(
                    ctx.as_ref(),
                    latest_table_info,
                    &self.meta_location_generator,
                    snapshot_tobe_committed,
                    None,
                    &None,
                    &self.operator,
                )
                .await
            {
                Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
                    match backoff.next_backoff() {
                        Some(d) => {
                            let name = self.table_info.name.clone();
                            debug!(
                                "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                d.as_millis(),
                                name.as_str(),
                                self.table_info.ident
                            );

                            tokio::time::sleep(d).await;
                            latest_table_ref = self.refresh(ctx.as_ref()).await?;
                            let latest_fuse_table =
                                FuseTable::try_from_table(latest_table_ref.as_ref())?;
                            latest_snapshot =
                                latest_fuse_table
                                    .read_table_snapshot()
                                    .await?
                                    .ok_or_else(|| {
                                        ErrorCode::Internal(
                                            "mutation meets empty snapshot during conflict reconciliation",
                                        )
                                    })?;
                            latest_table_info = &latest_fuse_table.table_info;
                            latest_schema = latest_fuse_table.schema();

                            // Check if there is only insertion during the operation.
                            if let Some(range_of_newly_append) =
                                ConflictResolveContext::is_latest_snapshot_append_only(
                                    &base_snapshot,
                                    &latest_snapshot,
                                )
                            {
                                info!("resolvable conflicts detected");
                                metrics_inc_commit_mutation_latest_snapshot_append_only();
                                concurrently_appended_segment_locations =
                                    &latest_snapshot.segments[range_of_newly_append];
                            } else {
                                metrics_inc_commit_mutation_unresolvable_conflict();
                                break Err(ErrorCode::UnresolvableConflict(
                                    "segment compact conflict with other operations",
                                ));
                            }

                            retries += 1;
                            metrics_inc_commit_mutation_retry();
                            continue;
                        }
                        None => {
                            // Commit not fulfilled, abort.
                            //
                            // Note that, here the last error we have seen is TableVersionMismatched,
                            // otherwise we should have been returned, thus it is safe to abort the operation here.
                            break Err(ErrorCode::StorageOther(format!(
                                "commit mutation failed after {} retries",
                                retries
                            )));
                        }
                    }
                }
                Err(e) => {
                    // we are not sure about if the table state has been modified or not, just propagate the error
                    // and return, without aborting anything.
                    break Err(e);
                }
                Ok(_) => {
                    break {
                        metrics_inc_commit_mutation_success();
                        Ok(())
                    };
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn merge_with_base(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        base_segments: &[Location],
        base_summary: &Statistics,
        concurrently_appended_segment_locations: &[Location],
        schema: TableSchemaRef,
        default_cluster_key_id: Option<u32>,
    ) -> Result<(Vec<Location>, Statistics)> {
        if concurrently_appended_segment_locations.is_empty() {
            Ok((base_segments.to_owned(), base_summary.clone()))
        } else {
            // place the concurrently appended segments at the head of segment list
            let new_segments = concurrently_appended_segment_locations
                .iter()
                .chain(base_segments.iter())
                .cloned()
                .collect();

            let fuse_segment_io = SegmentsIO::create(ctx, operator, schema);
            let concurrent_appended_segment_infos = fuse_segment_io
                .read_segments::<SegmentInfo>(concurrently_appended_segment_locations, true)
                .await?;

            let mut new_statistics = base_summary.clone();
            for result in concurrent_appended_segment_infos.into_iter() {
                let concurrent_appended_segment = result?;
                new_statistics = merge_statistics(
                    new_statistics.clone(),
                    &concurrent_appended_segment.summary,
                    default_cluster_key_id,
                );
            }
            Ok((new_segments, new_statistics))
        }
    }

    // check if there are any fuse table legacy options
    fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
        table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    }

    pub(crate) async fn generate_table_stats(
        &self,
        snapshot: &Option<Arc<TableSnapshot>>,
        insert_hll: &BlockHLL,
        insert_rows: u64,
    ) -> Result<TableStatsGenerator> {
        // Extract previous stats meta (row_count / hll, etc.) from snapshot
        let summary = snapshot.summary();
        let prev_stats_meta = summary.additional_stats_meta.as_ref();
        // Previous statistics file location (if any)
        let mut prev_stats_location = snapshot.table_statistics_location();
        // If no new rows are inserted, or HLL is empty, just reuse previous statistics
        if insert_rows == 0 || insert_hll.is_empty() {
            return Ok(TableStatsGenerator::new(
                prev_stats_meta.cloned(),
                prev_stats_location,
                0,
                0,
                HashMap::new(),
            ));
        }

        // Initialize a new HLL with inserted rows
        let mut new_hll = insert_hll.clone();
        // Calculate updated row_count
        let (row_count, unstats_rows) = match prev_stats_meta {
            // Case 1: Previous stats exist and already contain HLL → merge directly
            Some(v) if v.hll.is_some() => {
                let prev_hll = decode_column_hll(v.hll.as_ref().unwrap())?.unwrap();
                merge_column_hll_mut(&mut new_hll, &prev_hll);
                (v.row_count + insert_rows, v.unstats_rows)
            }
            // Case 2: Previous meta has no HLL → need to load from stats file
            _ => {
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
                            self.get_branch_id(),
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
                            // Successfully loaded the previous snapshot → use its row_count + inserted rows
                            merge_column_hll_mut(&mut new_hll, &prev_stats.hll);
                            let prev_rows = prev.summary.row_count;
                            (
                                prev_rows + insert_rows,
                                summary.row_count.saturating_sub(prev_rows),
                            )
                        } else {
                            // Could not load previous snapshot → old stats are invalid
                            // Drop prev_stats_location to mark stats as "reset",
                            // and only use inserted rows as the new base.
                            prev_stats_location = None;
                            (insert_rows, summary.row_count)
                        }
                    } else {
                        // Normal case: accumulate old row_count + inserted rows
                        merge_column_hll_mut(&mut new_hll, &prev_stats.hll);
                        (
                            prev_stats.row_count + insert_rows,
                            summary.row_count.saturating_sub(prev_stats.row_count),
                        )
                    }
                } else {
                    // No previous stats available → start from inserted rows only
                    (insert_rows, summary.row_count)
                }
            }
        };

        Ok(TableStatsGenerator::new(
            None,
            prev_stats_location,
            row_count,
            unstats_rows,
            new_hll,
        ))
    }
}
