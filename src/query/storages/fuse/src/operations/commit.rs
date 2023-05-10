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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use common_base::base::ProgressValues;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::MatchSeq;
use common_sql::field_default_value;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use tracing::debug;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use crate::io::MetaWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_inc_commit_mutation_resolvable_conflict;
use crate::metrics::metrics_inc_commit_mutation_retry;
use crate::metrics::metrics_inc_commit_mutation_success;
use crate::metrics::metrics_inc_commit_mutation_unresolvable_conflict;
use crate::operations::commit::utils::no_side_effects_in_meta_store;
use crate::operations::mutation::AbortOperation;
use crate::operations::AppendOperationLogEntry;
use crate::operations::TableOperationLog;
use crate::statistics;
use crate::statistics::merge_statistics;
use crate::FuseTable;

const OCC_DEFAULT_BACKOFF_INIT_DELAY_MS: Duration = Duration::from_millis(5);
const OCC_DEFAULT_BACKOFF_MAX_DELAY_MS: Duration = Duration::from_millis(20 * 1000);
const OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS: Duration = Duration::from_millis(120 * 1000);
const MAX_RETRIES: u64 = 10;

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_commit(
        &self,
        ctx: Arc<dyn TableContext>,
        operation_log: TableOperationLog,
        copied_files: Option<UpsertTableCopiedFileReq>,
        overwrite: bool,
    ) -> Result<()> {
        self.commit_with_max_retry_elapsed(ctx, operation_log, copied_files, None, overwrite)
            .await
    }

    #[async_backtrace::framed]
    pub async fn commit_with_max_retry_elapsed(
        &self,
        ctx: Arc<dyn TableContext>,
        operation_log: TableOperationLog,
        copied_files: Option<UpsertTableCopiedFileReq>,
        max_retry_elapsed: Option<Duration>,
        overwrite: bool,
    ) -> Result<()> {
        let mut tbl = self;
        let mut latest: Arc<dyn Table>;

        let mut retry_times = 0;

        // The initial retry delay in millisecond. By default,  it is 5 ms.
        let init_delay = OCC_DEFAULT_BACKOFF_INIT_DELAY_MS;

        // The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing.
        // By default, it is 20 seconds.
        let max_delay = OCC_DEFAULT_BACKOFF_MAX_DELAY_MS;

        // The maximum elapsed time after the occ starts, beyond which there will be no more retries.
        // By default, it is 2 minutes
        let max_elapsed = max_retry_elapsed.unwrap_or(OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS);

        // TODO(xuanwo): move to backon instead.
        //
        // To simplify the settings, using fixed common values for randomization_factor and multiplier
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(init_delay)
            .with_max_interval(max_delay)
            .with_randomization_factor(0.5)
            .with_multiplier(2.0)
            .with_max_elapsed_time(Some(max_elapsed))
            .build();

        let transient = self.transient();
        loop {
            match tbl
                .try_commit(ctx.clone(), &operation_log, &copied_files, overwrite)
                .await
            {
                Ok(_) => {
                    break {
                        if transient {
                            // Removes historical data, if table is transient
                            warn!(
                                "transient table detected, purging historical data. ({})",
                                tbl.table_info.ident
                            );

                            let latest = tbl.refresh(ctx.as_ref()).await?;
                            tbl = FuseTable::try_from_table(latest.as_ref())?;

                            let keep_last_snapshot = true;
                            let snapshot_files = self.list_snapshot_files().await?;
                            if let Err(e) = tbl
                                .do_purge(&ctx, snapshot_files, keep_last_snapshot, None)
                                .await
                            {
                                // Errors of GC, if any, are ignored, since GC task can be picked up
                                warn!(
                                    "GC of transient table not success (this is not a permanent error). the error : {}",
                                    e
                                );
                            } else {
                                info!("GC of transient table done");
                            }
                        }
                        Ok(())
                    };
                }
                Err(e) if self::utils::is_error_recoverable(&e, transient) => {
                    match backoff.next_backoff() {
                        Some(d) => {
                            let name = tbl.table_info.name.clone();
                            debug!(
                                "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                d.as_millis(),
                                name.as_str(),
                                tbl.table_info.ident
                            );
                            common_base::base::tokio::time::sleep(d).await;
                            latest = tbl.refresh(ctx.as_ref()).await?;
                            tbl = FuseTable::try_from_table(latest.as_ref())?;
                            retry_times += 1;
                            continue;
                        }
                        None => {
                            // Commit not fulfilled. try to abort the operations.
                            // if it is safe to do so.
                            if no_side_effects_in_meta_store(&e) {
                                // if we are sure that table state inside metastore has not been
                                // modified by this operation, abort this operation.
                                info!("aborting operations");
                                let _ = utils::abort_operations(self.get_operator(), operation_log)
                                    .await;
                            }
                            break Err(ErrorCode::OCCRetryFailure(format!(
                                "can not fulfill the tx after retries({} times, {} ms), aborted. table name {}, identity {}",
                                retry_times,
                                Instant::now()
                                    .duration_since(backoff.start_time)
                                    .as_millis(),
                                tbl.table_info.name.as_str(),
                                tbl.table_info.ident,
                            )));
                        }
                    }
                }

                Err(e) => {
                    // we are not sure about if the table state has been modified or not, just propagate the error
                    // and return, without aborting anything.
                    break Err(e);
                }
            }
        }
    }

    #[inline]
    #[async_backtrace::framed]
    pub async fn try_commit<'a>(
        &'a self,
        ctx: Arc<dyn TableContext>,
        operation_log: &'a TableOperationLog,
        copied_files: &Option<UpsertTableCopiedFileReq>,
        overwrite: bool,
    ) -> Result<()> {
        let prev = self.read_table_snapshot().await?;
        let prev_version = self.snapshot_format_version(None).await?;
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let prev_statistics_location = prev
            .as_ref()
            .and_then(|v| v.table_statistics_location.clone());
        let schema = self.table_info.meta.schema.as_ref().clone();
        let (segments, summary) = Self::merge_append_operations(operation_log)?;

        let progress_values = ProgressValues {
            rows: summary.row_count as usize,
            bytes: summary.uncompressed_byte_size as usize,
        };
        ctx.get_write_progress().incr(&progress_values);

        let segments = segments
            .into_iter()
            .map(|loc| (loc, SegmentInfo::VERSION))
            .collect();

        let new_snapshot = if overwrite {
            TableSnapshot::new(
                Uuid::new_v4(),
                &prev_timestamp,
                prev.as_ref().map(|v| (v.snapshot_id, prev_version)),
                schema,
                summary,
                segments,
                self.cluster_key_meta.clone(),
                prev_statistics_location,
            )
        } else {
            Self::merge_table_operations(
                self.table_info.meta.schema.as_ref(),
                ctx.clone(),
                prev,
                prev_version,
                segments,
                summary,
                self.cluster_key_meta.clone(),
            )?
        };

        let mut new_table_meta = self.get_table_info().meta.clone();
        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: new_snapshot.summary.row_count,
            data_bytes: new_snapshot.summary.uncompressed_byte_size,
            compressed_data_bytes: new_snapshot.summary.compressed_byte_size,
            index_data_bytes: new_snapshot.summary.index_size,
        };

        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            &self.table_info,
            &self.meta_location_generator,
            new_snapshot,
            None,
            copied_files,
            &self.operator,
        )
        .await
    }

    fn merge_table_operations(
        schema: &TableSchema,
        ctx: Arc<dyn TableContext>,
        previous: Option<Arc<TableSnapshot>>,
        prev_version: u64,
        mut new_segments: Vec<Location>,
        statistics: Statistics,
        cluster_key_meta: Option<ClusterKey>,
    ) -> Result<TableSnapshot> {
        // 1. merge stats with previous snapshot, if any
        let stats = if let Some(snapshot) = &previous {
            let mut summary = snapshot.summary.clone();
            let mut fill_default_values = false;
            // check if need to fill default value in statistics
            for column_id in statistics.col_stats.keys() {
                if !summary.col_stats.contains_key(column_id) {
                    fill_default_values = true;
                    break;
                }
            }
            if fill_default_values {
                let mut default_values = Vec::with_capacity(schema.num_fields());
                for field in schema.fields() {
                    default_values.push(field_default_value(ctx.clone(), field)?);
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

            merge_statistics(&statistics, &summary)?
        } else {
            statistics
        };
        let prev_snapshot_id = previous.as_ref().map(|v| (v.snapshot_id, prev_version));
        let prev_snapshot_timestamp = previous.as_ref().and_then(|v| v.timestamp);
        let prev_statistics_location = previous
            .as_ref()
            .and_then(|v| v.table_statistics_location.clone());

        // 2. merge segment locations with previous snapshot, if any
        if let Some(snapshot) = &previous {
            let mut segments = snapshot.segments.clone();
            new_segments.append(&mut segments)
        };

        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &prev_snapshot_timestamp,
            prev_snapshot_id,
            schema.clone(),
            stats,
            new_segments,
            cluster_key_meta,
            prev_statistics_location,
        );
        Ok(new_snapshot)
    }

    #[async_backtrace::framed]
    pub async fn commit_to_meta_server(
        ctx: &dyn TableContext,
        table_info: &TableInfo,
        location_generator: &TableMetaLocationGenerator,
        snapshot: TableSnapshot,
        table_statistics: Option<TableSnapshotStatistics>,
        copied_files: &Option<UpsertTableCopiedFileReq>,
        operator: &Operator,
    ) -> Result<()> {
        let snapshot_location = location_generator
            .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshot::VERSION)?;
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

        // 2. prepare table meta
        let mut new_table_meta = table_info.meta.clone();
        // 2.1 set new snapshot location
        new_table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
            snapshot_location.clone(),
        );
        // remove legacy options
        utils::remove_legacy_options(&mut new_table_meta.options);

        // 2.2 setup table statistics
        let stats = &snapshot.summary;
        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: stats.row_count,
            data_bytes: stats.uncompressed_byte_size,
            compressed_data_bytes: stats.compressed_byte_size,
            index_data_bytes: stats.index_size,
        };

        // 3. prepare the request
        let catalog = ctx.get_catalog(&table_info.meta.catalog)?;
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            copied_files: copied_files.clone(),
        };

        // 3. let's roll
        let reply = catalog.update_table_meta(table_info, req).await;
        match reply {
            Ok(_) => {
                // upsert snapshot statistics cache
                if let Some(snapshot_statistics) = table_statistics {
                    if let Some(location) = &snapshot.table_statistics_location {
                        TableSnapshotStatistics::cache()
                            .put(location.clone(), Arc::new(snapshot_statistics));
                    }
                }
                TableSnapshot::cache().put(snapshot_location.clone(), Arc::new(snapshot));
                // try keep a hit file of last snapshot
                Self::write_last_snapshot_hint(operator, location_generator, snapshot_location)
                    .await;
                Ok(())
            }
            Err(e) => {
                // commit snapshot to meta server failed.
                // figure out if the un-committed snapshot is safe to be removed.
                if no_side_effects_in_meta_store(&e) {
                    // currently, only in this case (TableVersionMismatched),  we are SURE about
                    // that the table state insides meta store has NOT been changed.
                    info!(
                        "removing uncommitted table snapshot at location {}, of table {}, {}",
                        snapshot_location, table_info.desc, table_info.ident
                    );
                    let _ = operator.delete(&snapshot_location).await;
                    if need_to_save_statistics {
                        let _ = operator
                            .delete(&snapshot.table_statistics_location.unwrap())
                            .await;
                    }
                }
                Err(e)
            }
        }
    }

    pub fn merge_append_operations(
        append_log_entries: &[AppendOperationLogEntry],
    ) -> Result<(Vec<String>, Statistics)> {
        let iter = append_log_entries
            .iter()
            .map(|entry| (&entry.segment_location, entry.segment_info.as_ref()));
        FuseTable::merge_segments(iter)
    }

    pub fn merge_segments<'a, T>(
        mut segments: impl Iterator<Item = (&'a T, &'a SegmentInfo)>,
    ) -> Result<(Vec<T>, Statistics)>
    where T: Clone + 'a {
        let len_hint = segments
            .size_hint()
            .1
            .unwrap_or_else(|| segments.size_hint().0);
        let (s, seg_locs) = segments.try_fold(
            (Statistics::default(), Vec::with_capacity(len_hint)),
            |(mut acc, mut seg_acc), (location, segment_info)| {
                let stats = &segment_info.summary;
                acc.row_count += stats.row_count;
                acc.block_count += stats.block_count;
                acc.perfect_block_count += stats.perfect_block_count;
                acc.uncompressed_byte_size += stats.uncompressed_byte_size;
                acc.compressed_byte_size += stats.compressed_byte_size;
                acc.index_size += stats.index_size;
                acc.col_stats = if acc.col_stats.is_empty() {
                    stats.col_stats.clone()
                } else {
                    statistics::reduce_block_statistics(&[&acc.col_stats, &stats.col_stats])?
                };
                seg_acc.push(location.clone());
                Ok::<_, ErrorCode>((acc, seg_acc))
            },
        )?;

        Ok((seg_locs, s))
    }

    // Left a hint file which indicates the location of the latest snapshot
    #[async_backtrace::framed]
    pub async fn write_last_snapshot_hint(
        operator: &Operator,
        location_generator: &TableMetaLocationGenerator,
        last_snapshot_path: String,
    ) {
        // Just try our best to write down the hint file of last snapshot
        // - will retry in the case of temporary failure
        // but
        // - errors are ignored if writing is eventually failed
        // - errors (if any) will not be propagated to caller
        // - "data race" ignored
        //   if multiple different versions of hints are written concurrently
        //   it is NOT guaranteed that the latest version will be kept

        let hint_path = location_generator.gen_last_snapshot_hint_location();
        let last_snapshot_path = {
            let operator_meta_data = operator.info();
            let storage_prefix = operator_meta_data.root();
            format!("{}{}", storage_prefix, last_snapshot_path)
        };

        operator
            .write(&hint_path, last_snapshot_path)
            .await
            .unwrap_or_else(|e| {
                warn!("write last snapshot hint failure. {}", e);
            });
    }

    // TODO refactor, it is called by segment compaction and re-cluster now
    #[async_backtrace::framed]
    pub async fn commit_mutation(
        &self,
        ctx: &Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        base_segments: Vec<Location>,
        base_summary: Statistics,
        abort_operation: AbortOperation,
    ) -> Result<()> {
        let mut retries = 0;

        let mut latest_snapshot = base_snapshot.clone();
        let mut latest_table_info = &self.table_info;

        // holding the reference of latest table during retries
        let mut latest_table_ref: Arc<dyn Table>;

        // potentially concurrently appended segments, init it to empty
        let mut concurrently_appended_segment_locations: &[Location] = &[];

        // Status
        {
            let status = "mutation: begin try to commit";
            ctx.set_status_info(status);
            info!(status);
        }

        while retries < MAX_RETRIES {
            let mut snapshot_tobe_committed =
                TableSnapshot::from_previous(latest_snapshot.as_ref());

            let schema = self.schema();
            let (segments_tobe_committed, statistics_tobe_committed) = Self::merge_with_base(
                ctx.clone(),
                self.operator.clone(),
                &base_segments,
                &base_summary,
                concurrently_appended_segment_locations,
                schema,
            )
            .await?;
            snapshot_tobe_committed.segments = segments_tobe_committed;
            snapshot_tobe_committed.summary = statistics_tobe_committed;

            match Self::commit_to_meta_server(
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
                    latest_table_ref = self.refresh(ctx.as_ref()).await?;
                    let latest_fuse_table = FuseTable::try_from_table(latest_table_ref.as_ref())?;
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

                    // Check if there is only insertion during the operation.
                    match MutatorConflictDetector::detect_conflicts(
                        base_snapshot.as_ref(),
                        latest_snapshot.as_ref(),
                    ) {
                        Conflict::Unresolvable => {
                            abort_operation
                                .abort(ctx.clone(), self.operator.clone())
                                .await?;
                            metrics_inc_commit_mutation_unresolvable_conflict();
                            return Err(ErrorCode::StorageOther(
                                "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                            ));
                        }
                        Conflict::ResolvableAppend(range_of_newly_append) => {
                            info!("resolvable conflicts detected");
                            metrics_inc_commit_mutation_resolvable_conflict();
                            concurrently_appended_segment_locations =
                                &latest_snapshot.segments[range_of_newly_append];
                        }
                    }

                    retries += 1;
                    metrics_inc_commit_mutation_retry();
                }
                Err(e) => {
                    // we are not sure about if the table state has been modified or not, just propagate the error
                    // and return, without aborting anything.
                    return Err(e);
                }
                Ok(_) => {
                    return {
                        metrics_inc_commit_mutation_success();
                        Ok(())
                    };
                }
            }
        }

        // Commit not fulfilled. try to abort the operations.
        //
        // Note that, here the last error we have seen is TableVersionMismatched,
        // otherwise we should have been returned, thus it is safe to abort the operation here.
        abort_operation
            .abort(ctx.clone(), self.operator.clone())
            .await?;
        Err(ErrorCode::StorageOther(format!(
            "commit mutation failed after {} retries",
            retries
        )))
    }

    #[async_backtrace::framed]
    async fn merge_with_base(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        base_segments: &[Location],
        base_summary: &Statistics,
        concurrently_appended_segment_locations: &[Location],
        schema: TableSchemaRef,
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
                .read_segments(concurrently_appended_segment_locations, true)
                .await?;

            let mut new_statistics = base_summary.clone();
            for result in concurrent_appended_segment_infos.into_iter() {
                let concurrent_appended_segment = result?;
                new_statistics =
                    merge_statistics(&new_statistics, &concurrent_appended_segment.summary)?;
            }
            Ok((new_segments, new_statistics))
        }
    }
}

pub enum Conflict {
    Unresolvable,
    // resolvable conflicts with append only operation
    // the range embedded is the range of segments that are appended in the latest snapshot
    ResolvableAppend(Range<usize>),
}

// wraps a namespace, to clarify the who is detecting conflict
pub struct MutatorConflictDetector;

impl MutatorConflictDetector {
    // detects conflicts, as a mutator, working on the base snapshot, with latest snapshot
    pub fn detect_conflicts(base: &TableSnapshot, latest: &TableSnapshot) -> Conflict {
        let base_segments = &base.segments;
        let latest_segments = &latest.segments;

        let base_segments_len = base_segments.len();
        let latest_segments_len = latest_segments.len();

        if latest_segments_len >= base_segments_len
            && base_segments[0..base_segments_len]
                == latest_segments[(latest_segments_len - base_segments_len)..latest_segments_len]
        {
            Conflict::ResolvableAppend(0..(latest_segments_len - base_segments_len))
        } else {
            Conflict::Unresolvable
        }
    }
}

mod utils {
    use std::collections::BTreeMap;

    use storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;

    use super::*;
    use crate::metrics::metrics_inc_commit_mutation_aborts;

    #[inline]
    #[async_backtrace::framed]
    pub async fn abort_operations(
        operator: Operator,
        operation_log: TableOperationLog,
    ) -> Result<()> {
        metrics_inc_commit_mutation_aborts();
        for entry in operation_log {
            for block in &entry.segment_info.blocks {
                let block_location = &block.location.0;
                // if deletion operation failed (after DAL retried)
                // we just left them there, and let the "major GC" collect them
                info!(
                    "aborting operation, delete block location: {:?}",
                    block_location,
                );
                let _ = operator.delete(block_location).await;
                if let Some(index) = &block.bloom_filter_index_location {
                    info!(
                        "aborting operation, delete bloom index location: {:?}",
                        index.0
                    );
                    let _ = operator.delete(&index.0).await;
                }
            }
            info!(
                "aborting operation, delete segment location: {:?}",
                entry.segment_location
            );
            let _ = operator.delete(&entry.segment_location).await;
        }
        Ok(())
    }

    #[inline]
    pub fn is_error_recoverable(e: &ErrorCode, is_table_transient: bool) -> bool {
        let code = e.code();
        code == ErrorCode::TABLE_VERSION_MISMATCHED
            || (is_table_transient && code == ErrorCode::STORAGE_NOT_FOUND)
    }

    #[inline]
    pub fn no_side_effects_in_meta_store(e: &ErrorCode) -> bool {
        // currently, the only error that we know,  which indicates there are no side effects
        // is TABLE_VERSION_MISMATCHED
        e.code() == ErrorCode::TABLE_VERSION_MISMATCHED
    }

    // check if there are any fuse table legacy options
    pub fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
        table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    }
}
