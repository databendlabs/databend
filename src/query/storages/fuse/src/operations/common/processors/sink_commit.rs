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
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::VirtualDataSchema;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::TruncateMode;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use log::debug;
use log::error;
use log::info;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::set_backoff;
use crate::operations::vacuum::vacuum_table;
use crate::operations::AppendGenerator;
use crate::operations::CommitMeta;
use crate::operations::MutationGenerator;
use crate::operations::SnapshotGenerator;
use crate::operations::TransformMergeCommitMeta;
use crate::operations::TruncateGenerator;
use crate::statistics::TableStatsGenerator;
use crate::FuseTable;
use crate::FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE;
use crate::FUSE_OPT_KEY_ENABLE_AUTO_VACUUM;

enum State {
    None,
    FillDefault,
    RefreshTable,
    GenerateSnapshot {
        previous: Option<Arc<TableSnapshot>>,
        table_stats_gen: TableStatsGenerator,
        cluster_key_id: Option<u32>,
        table_info: TableInfo,
    },
    TryCommit {
        data: Vec<u8>,
        snapshot: TableSnapshot,
        table_info: TableInfo,
    },
    Abort(ErrorCode),
    Finish,
}

// Gathers all the segments and commits to the meta server.
pub struct CommitSink<F: SnapshotGenerator> {
    state: State,

    input: Arc<InputPort>,

    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    table: Arc<dyn Table>,
    copied_files: Option<UpsertTableCopiedFileReq>,
    snapshot_gen: F,
    purge_mode: Option<PurgeMode>,
    retries: u64,
    max_retry_elapsed: Option<Duration>,
    backoff: ExponentialBackoff,

    new_segment_locs: Vec<Location>,
    new_virtual_schema: Option<VirtualDataSchema>,
    start_time: Instant,
    prev_snapshot_id: Option<SnapshotId>,
    insert_hll: BlockHLL,
    insert_rows: u64,
    enable_auto_analyze: bool,

    change_tracking: bool,
    update_stream_meta: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
    table_meta_timestamps: TableMetaTimestamps,
    vacuum_handler: Option<Arc<VacuumHandlerWrapper>>,
}

#[derive(Debug)]
enum PurgeMode {
    PurgeAllHistory,
    PurgeAccordingToRetention,
}

impl<F> CommitSink<F>
where F: SnapshotGenerator + Send + Sync + 'static
{
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        snapshot_gen: F,
        input: Arc<InputPort>,
        max_retry_elapsed: Option<Duration>,
        prev_snapshot_id: Option<SnapshotId>,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<ProcessorPtr> {
        let purge_mode = Self::purge_mode(ctx.as_ref(), table, &snapshot_gen)?;
        let enable_auto_analyze = Self::enable_auto_analyze(table, &snapshot_gen);

        let vacuum_handler = if LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Vacuum)
            .is_ok()
        {
            let handler: Arc<VacuumHandlerWrapper> = GlobalInstance::get();
            Some(handler)
        } else {
            None
        };

        Ok(ProcessorPtr::create(Box::new(CommitSink {
            state: State::None,
            ctx,
            dal: table.get_operator(),
            location_gen: table.meta_location_generator.clone(),
            table: Arc::new(table.clone()),
            copied_files,
            snapshot_gen,
            purge_mode,
            backoff: ExponentialBackoff::default(),
            retries: 0,
            max_retry_elapsed,
            input,
            new_segment_locs: vec![],
            new_virtual_schema: None,
            insert_hll: HashMap::new(),
            insert_rows: 0,
            start_time: Instant::now(),
            enable_auto_analyze,
            prev_snapshot_id,
            change_tracking: table.change_tracking_enabled(),
            update_stream_meta,
            deduplicated_label,
            table_meta_timestamps,
            vacuum_handler,
        })))
    }

    fn purge_mode(
        ctx: &dyn TableContext,
        table: &FuseTable,
        snapshot_gen: &F,
    ) -> Result<Option<PurgeMode>> {
        let mode = if Self::need_to_purge_all_history(table, snapshot_gen) {
            Some(PurgeMode::PurgeAllHistory)
        } else if Self::is_auto_vacuum_enabled(ctx, table)? {
            Some(PurgeMode::PurgeAccordingToRetention)
        } else {
            None
        };
        Ok(mode)
    }

    fn enable_auto_analyze(table: &FuseTable, snapshot_gen: &F) -> bool {
        let enable_auto_analyze = table.get_option(FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE, 0u32);
        if enable_auto_analyze == 0 {
            return false;
        }

        snapshot_gen
            .as_any()
            .downcast_ref::<MutationGenerator>()
            .is_some_and(|gen| {
                matches!(
                    gen.mutation_kind,
                    MutationKind::Update
                        | MutationKind::Delete
                        | MutationKind::MergeInto
                        | MutationKind::Replace
                )
            })
    }

    fn is_auto_vacuum_enabled(ctx: &dyn TableContext, table: &FuseTable) -> Result<bool> {
        // Priority for auto vacuum:
        // - If table-level option `FUSE_OPT_KEY_ENABLE_AUTO_VACUUM` is set, it takes precedence
        // - If table-level option is not set, fall back to the setting
        match table
            .table_info
            .options()
            .get(FUSE_OPT_KEY_ENABLE_AUTO_VACUUM)
        {
            Some(v) => {
                let enabled = v.parse::<u32>()? != 0;
                Ok(enabled)
            }
            None => ctx.get_settings().get_enable_auto_vacuum(),
        }
    }

    fn is_error_recoverable(&self, e: &ErrorCode) -> bool {
        let code = e.code();
        // When prev_snapshot_id is some, means it is an alter table column modification or truncate.
        if self.prev_snapshot_id.is_some() && code == ErrorCode::TABLE_VERSION_MISMATCHED {
            // In this case if commit to meta fail and error is TABLE_VERSION_MISMATCHED operation will be aborted.
            return false;
        }

        code == ErrorCode::TABLE_VERSION_MISMATCHED
            || (self.purge_mode.is_some() && code == ErrorCode::STORAGE_NOT_FOUND)
    }

    fn no_side_effects_in_meta_store(e: &ErrorCode) -> bool {
        // currently, the only error that we know,  which indicates there are no side effects
        // is TABLE_VERSION_MISMATCHED
        e.code() == ErrorCode::TABLE_VERSION_MISMATCHED
    }

    fn read_meta(&mut self) -> Result<Event> {
        self.start_time = Instant::now();
        {
            self.ctx.set_status_info("begin commit");
        }

        let input_meta = self
            .input
            .pull_data()
            .unwrap()?
            .get_meta()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;

        self.input.finish();

        let meta = CommitMeta::downcast_from(input_meta)
            .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?;

        self.new_segment_locs = meta.new_segment_locs;

        self.new_virtual_schema = meta.virtual_schema;

        if !meta.hll.is_empty() {
            let binding = self.ctx.get_mutation_status();
            let status = binding.read();
            self.insert_rows = status.insert_rows + status.update_rows;
            self.insert_hll = meta.hll;
        }

        self.backoff = set_backoff(None, None, self.max_retry_elapsed);

        self.snapshot_gen
            .set_conflict_resolve_context(meta.conflict_resolve_context);
        self.state = State::FillDefault;

        Ok(Event::Async)
    }

    fn need_to_purge_all_history(table: &FuseTable, snapshot_gen: &F) -> bool {
        if table.is_transient() {
            return true;
        }

        snapshot_gen
            .as_any()
            .downcast_ref::<TruncateGenerator>()
            .is_some_and(|gen| matches!(gen.mode(), TruncateMode::DropAll))
    }

    fn need_truncate(&self) -> bool {
        self.snapshot_gen
            .as_any()
            .downcast_ref::<TruncateGenerator>()
            .is_some_and(|gen| !matches!(gen.mode(), TruncateMode::Delete))
    }

    fn is_append_only_txn(&self) -> bool {
        self.snapshot_gen
            .as_any()
            .downcast_ref::<AppendGenerator>()
            .is_some()
    }

    async fn clean_history(&self, purge_mode: &PurgeMode) -> Result<()> {
        {
            let table_info = self.table.get_table_info();
            info!(
                "cleaning historical data. table: {}, ident: {}, purge_mode {:?}",
                table_info.desc, table_info.ident, purge_mode
            );
        }

        let latest = self.table.refresh(self.ctx.as_ref()).await?;
        let tbl = FuseTable::try_from_table(latest.as_ref())?;

        if let Some(vacuum_handler) = &self.vacuum_handler {
            let respect_flash_back = true;
            vacuum_table(tbl, self.ctx.clone(), vacuum_handler, respect_flash_back).await;
        } else {
            info!("[SINK-COMMIT] No vacuum handler available for auto vacuuming, please verify your license");
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<F> Processor for CommitSink<F>
where F: SnapshotGenerator + Send + Sync + 'static
{
    fn name(&self) -> String {
        "CommitSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::GenerateSnapshot { .. } | State::Abort(_)
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::FillDefault | State::TryCommit { .. } | State::RefreshTable
        ) {
            return Ok(Event::Async);
        }

        if matches!(self.state, State::Finish) {
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.read_meta()
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GenerateSnapshot {
                previous,
                table_stats_gen,
                cluster_key_id,
                table_info,
            } => {
                let change_tracking_enabled_during_commit = {
                    let no_change_tracking_at_beginning = !self.change_tracking;
                    // note that `self.table` might be refreshed if commit retried
                    let change_tracking_enabled_now = self.table.change_tracking_enabled();

                    no_change_tracking_at_beginning && change_tracking_enabled_now
                };

                if !self.is_append_only_txn() && change_tracking_enabled_during_commit {
                    // If change tracking is not enabled when the txn start, but is enabled when committing,
                    // then the txn should be aborted.
                    // For mutations other than append-only, stream column values (like _origin_block_id)
                    // must be properly generated. If not, CDC will not function as expected.
                    self.state = State::Abort(ErrorCode::StorageOther(
                        "commit failed because change tracking was enabled during the commit process",
                    ));
                    return Ok(());
                }

                // now:
                // - either current txn IS append-only
                //    even if this is a conflict txn T (in the meaning of table version) has been
                // committed, which has changed the change-tracking state from disabled to enabled,
                // merging with transaction T is still safe, since the CDC mechanism allows it.
                // - or change-tracking state is NOT changed.
                //    in this case, we only need standard conflict resolution.
                // therefore, we can safely proceed.

                match self.snapshot_gen.generate_new_snapshot(
                    &table_info,
                    cluster_key_id,
                    previous,
                    self.ctx.txn_mgr(),
                    self.table_meta_timestamps,
                    table_stats_gen,
                ) {
                    Ok(snapshot) => {
                        self.state = State::TryCommit {
                            data: snapshot.to_bytes()?,
                            snapshot,
                            table_info,
                        };
                    }
                    Err(e) => {
                        self.state = State::Abort(e);
                    }
                }
            }
            State::Abort(e) => {
                let duration = self.start_time.elapsed();
                metrics_inc_commit_aborts();
                metrics_inc_commit_milliseconds(duration.as_millis());
                error!(
                    "transaction aborted after {} retries, which took {} ms, cause: {:?}",
                    self.retries,
                    duration.as_millis(),
                    e
                );
                return Err(e);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::FillDefault => {
                let schema = self.table.schema().as_ref().clone();

                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?.to_owned();
                let previous = fuse_table.read_table_snapshot().await.map_err(|e| {
                    if e.code() == ErrorCode::STORAGE_NOT_FOUND {
                        e.add_message(
                            "Previous table snapshot not found. This could indicate the table is currently being vacuumed. Please check the settings for `data_retention_time_in_days` and the table option `data_retention_period_in_hours` to ensure they are not set too low.",
                        )
                    } else {
                        e
                    }
                })?;
                // save current table info when commit to meta server
                // if table_id not match, update table meta will fail
                let mut table_info = fuse_table.table_info.clone();

                // merge virtual schema
                let old_virtual_schema = std::mem::take(&mut table_info.meta.virtual_schema);
                let new_virtual_schema = std::mem::take(&mut self.new_virtual_schema);
                let merged_virtual_schema = TransformMergeCommitMeta::merge_virtual_schema(
                    old_virtual_schema,
                    new_virtual_schema,
                );
                table_info.meta.virtual_schema = merged_virtual_schema;

                // check if snapshot has been changed
                let snapshot_has_changed = self.prev_snapshot_id.is_some_and(|prev_snapshot_id| {
                    previous
                        .as_ref()
                        .is_none_or(|previous| previous.snapshot_id != prev_snapshot_id)
                });
                if snapshot_has_changed {
                    // if snapshot has changed abort operation
                    self.state = State::Abort(ErrorCode::StorageOther(
                        "commit failed because the snapshot had changed during the commit process",
                    ));
                } else {
                    self.snapshot_gen
                        .fill_default_values(schema, &previous)
                        .await?;
                    let table_stats_gen = fuse_table
                        .generate_table_stats(&previous, &self.insert_hll, self.insert_rows)
                        .await?;
                    self.state = State::GenerateSnapshot {
                        previous,
                        table_stats_gen,
                        cluster_key_id: fuse_table.cluster_key_id(),
                        table_info,
                    };
                }
            }
            State::TryCommit {
                data,
                snapshot,
                table_info,
            } => {
                let location = self
                    .location_gen
                    .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshot::VERSION)?;
                self.dal.write(&location, data).await?;

                // enable auto analyze.
                let mut enable_auto_analyze = false;
                if self.enable_auto_analyze {
                    if let Some(meta) = &snapshot.summary.additional_stats_meta {
                        let actual_rows =
                            snapshot.summary.row_count.saturating_sub(meta.unstats_rows);
                        let stats_rows = meta.row_count;
                        let diff = stats_rows.abs_diff(actual_rows);
                        enable_auto_analyze = diff * 20 >= actual_rows;
                    }
                }

                let catalog = self.ctx.get_catalog(table_info.catalog()).await?;
                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?;
                match fuse_table
                    .update_table_meta(
                        self.ctx.as_ref(),
                        catalog.clone(),
                        &table_info,
                        &self.location_gen,
                        snapshot,
                        location,
                        &self.copied_files,
                        &self.update_stream_meta,
                        &self.dal,
                        self.deduplicated_label.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        if self.need_truncate() {
                            // Truncate table operation should be executed in the context of ddl,
                            // which implies auto commit mode.
                            // Note that `catalog.truncate_table` may mutate table state in the meta server.
                            assert!(!self.ctx.txn_mgr().lock().is_active());
                            catalog
                                .truncate_table(&table_info, TruncateTableReq {
                                    table_id: table_info.ident.table_id,
                                    batch_size: None,
                                })
                                .await?;
                        }

                        if let Some(purge_mode) = &self.purge_mode {
                            // Flag to determine whether to vacuum data immediately or defer it
                            let mut purge_immediately = true;
                            {
                                let txn_mgr_ref = self.ctx.txn_mgr();
                                let mut txn_mgr = txn_mgr_ref.lock();
                                if txn_mgr.is_active() {
                                    // If inside an active transaction, schedule the table for purging after
                                    // the transaction completes
                                    txn_mgr.defer_table_purge(table_info);
                                    purge_immediately = false;
                                }
                            }
                            if purge_immediately {
                                // No inside an active transaction, safe to vacuum data immediately
                                self.clean_history(purge_mode).await?;
                            }
                        }

                        metrics_inc_commit_mutation_success();
                        {
                            let elapsed_time = self.start_time.elapsed();
                            let status = format!(
                                "[SINK-COMMIT] Mutation committed successfully after {} retries in {:?}",
                                self.retries, elapsed_time
                            );
                            metrics_inc_commit_milliseconds(elapsed_time.as_millis());
                            self.ctx.set_status_info(&status);
                        }
                        if let Some(files) = &self.copied_files {
                            metrics_inc_commit_copied_files(files.file_info.len() as u64);
                        }
                        for segment_loc in std::mem::take(&mut self.new_segment_locs).into_iter() {
                            self.ctx.add_written_segment_location(segment_loc)?;
                        }

                        if enable_auto_analyze {
                            self.ctx.set_enable_auto_analyze(true);
                        }
                        let target_descriptions = {
                            let table_info = self.table.get_table_info();
                            let tbl = (&table_info.name, table_info.ident, &table_info.meta.engine);

                            let stream_descriptions = self
                                .update_stream_meta
                                .iter()
                                .map(|s| (s.stream_id, s.seq, "stream"))
                                .collect::<Vec<_>>();
                            (tbl, stream_descriptions)
                        };
                        info!(
                            "[SINK-COMMIT] Mutation committed successfully, targets: {:?}",
                            target_descriptions
                        );
                        self.state = State::Finish;
                    }
                    Err(e) if self.is_error_recoverable(&e) => {
                        let table_info = self.table.get_table_info();
                        match self.backoff.next_backoff() {
                            Some(d) => {
                                let name = table_info.name.clone();
                                debug!(
                                    "[SINK-COMMIT] TableVersionMismatched error detected, transaction will retry in {} ms. Table: {}, ID: {}",
                                    d.as_millis(),
                                    name.as_str(),
                                    table_info.ident
                                );
                                databend_common_base::base::tokio::time::sleep(d).await;
                                self.retries += 1;
                                self.state = State::RefreshTable;
                            }
                            None => {
                                // Commit not fulfilled. try to abort the operations.
                                // if it is safe to do so.
                                if Self::no_side_effects_in_meta_store(&e) {
                                    // if we are sure that table state inside metastore has not been
                                    // modified by this operation, abort this operation.
                                    self.state = State::Abort(e);
                                } else {
                                    return Err(ErrorCode::OCCRetryFailure(format!(
                                        "can not fulfill the tx after retries({} times, {} ms), aborted. table name {}, identity {}",
                                        self.retries,
                                        Instant::now()
                                            .duration_since(self.backoff.start_time)
                                            .as_millis(),
                                        table_info.name.as_str(),
                                        table_info.ident,
                                    )));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // we are not sure about if the table state has been modified or not, just propagate the error
                        // and return, without aborting anything.
                        return Err(e);
                    }
                };
            }
            State::RefreshTable => {
                self.table = self.table.refresh(self.ctx.as_ref()).await?;
                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?.to_owned();
                let previous = fuse_table.read_table_snapshot().await?;
                let table_stats_gen = fuse_table
                    .generate_table_stats(&previous, &self.insert_hll, self.insert_rows)
                    .await?;
                self.state = State::GenerateSnapshot {
                    previous,
                    table_stats_gen,
                    cluster_key_id: fuse_table.cluster_key_id(),
                    table_info: fuse_table.table_info.clone(),
                };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
