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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use opendal::Operator;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use table_lock::TableLockHandlerWrapper;
use table_lock::TableLockHeartbeat;

use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_inc_commit_aborts;
use crate::metrics::metrics_inc_commit_mutation_success;
use crate::operations::merge_into::mutation_meta::CommitMeta;
use crate::operations::merge_into::processors::SnapshotGenerator;
use crate::operations::mutation::AbortOperation;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::FuseTable;

enum State {
    None,
    TryLock,
    RefreshTable,
    GenerateSnapshot {
        previous: Option<Arc<TableSnapshot>>,
        cluster_key_meta: Option<ClusterKey>,
    },
    TryCommit {
        data: Vec<u8>,
        snapshot: TableSnapshot,
    },
    AbortOperation,
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
    transient: bool,
    retries: u64,
    max_retry_elapsed: Option<Duration>,
    backoff: ExponentialBackoff,

    abort_operation: AbortOperation,
    heartbeat: TableLockHeartbeat,
}

impl<F> CommitSink<F>
where F: SnapshotGenerator + Send + 'static
{
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        copied_files: Option<UpsertTableCopiedFileReq>,
        snapshot_gen: F,
        input: Arc<InputPort>,
        max_retry_elapsed: Option<Duration>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(CommitSink {
            state: State::None,
            ctx,
            dal: table.get_operator(),
            location_gen: table.meta_location_generator.clone(),
            table: Arc::new(table.clone()),
            copied_files,
            snapshot_gen,
            abort_operation: AbortOperation::default(),
            heartbeat: TableLockHeartbeat::default(),
            transient: table.transient(),
            backoff: ExponentialBackoff::default(),
            retries: 0,
            max_retry_elapsed,
            input,
        })))
    }

    fn read_meta(&mut self) -> Result<Event> {
        {
            let status = "begin commit";
            self.ctx.set_status_info(status);
            tracing::info!(status);
        }

        let input_meta = self
            .input
            .pull_data()
            .unwrap()?
            .get_meta()
            .cloned()
            .ok_or(ErrorCode::Internal("No block meta. It's a bug"))?;

        self.input.finish();

        let meta = CommitMeta::downcast_ref_from(&input_meta)
            .ok_or(ErrorCode::Internal("No commit meta. It's a bug"))?;

        self.snapshot_gen.set_merged_segments(meta.segments.clone());
        self.snapshot_gen.set_merged_summary(meta.summary.clone());
        self.abort_operation = meta.abort_operation.clone();

        self.backoff = FuseTable::set_backoff(self.max_retry_elapsed);

        if meta.need_lock {
            self.state = State::TryLock;
        } else {
            self.state = State::RefreshTable;
        }

        Ok(Event::Async)
    }
}

#[async_trait::async_trait]
impl<F> Processor for CommitSink<F>
where F: SnapshotGenerator + Send + 'static
{
    fn name(&self) -> String {
        "MutationSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(&self.state, State::GenerateSnapshot { .. }) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::TryCommit { .. } | State::RefreshTable | State::AbortOperation
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
                cluster_key_meta,
            } => {
                let schema = self.table.schema().as_ref().clone();
                match self
                    .snapshot_gen
                    .generate_new_snapshot(schema, cluster_key_meta, previous)
                {
                    Ok(snapshot) => {
                        self.state = State::TryCommit {
                            data: snapshot.to_bytes()?,
                            snapshot,
                        };
                    }
                    Err(e) => {
                        tracing::error!(
                            "commit mutation failed after {} retries, error: {:?}",
                            self.retries,
                            e,
                        );
                        self.state = State::AbortOperation;
                    }
                }
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::TryLock => {
                let table_info = self.table.get_table_info();
                let handler = TableLockHandlerWrapper::instance(self.ctx.clone());
                match handler.try_lock(self.ctx.clone(), table_info.clone()).await {
                    Ok(heartbeat) => {
                        self.heartbeat = heartbeat;
                        self.state = State::RefreshTable;
                    }
                    Err(e) => {
                        tracing::error!(
                            "commit mutation failed cause get lock failed, error: {:?}",
                            e
                        );
                        self.state = State::AbortOperation;
                    }
                }
            }
            State::TryCommit { data, snapshot } => {
                let location = self
                    .location_gen
                    .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshot::VERSION)?;

                self.dal.write(&location, data).await?;

                match FuseTable::update_table_meta(
                    self.ctx.as_ref(),
                    self.table.get_table_info(),
                    &self.location_gen,
                    snapshot,
                    location,
                    &self.copied_files,
                    &self.dal,
                )
                .await
                {
                    Ok(_) => {
                        if self.transient {
                            // Removes historical data, if table is transient
                            let latest = self.table.refresh(self.ctx.as_ref()).await?;
                            let tbl = FuseTable::try_from_table(latest.as_ref())?;

                            tracing::warn!(
                                "transient table detected, purging historical data. ({})",
                                tbl.table_info.ident
                            );

                            let keep_last_snapshot = true;
                            let snapshot_files = tbl.list_snapshot_files().await?;
                            if let Err(e) = tbl
                                .do_purge(&self.ctx, snapshot_files, keep_last_snapshot, None)
                                .await
                            {
                                // Errors of GC, if any, are ignored, since GC task can be picked up
                                tracing::warn!(
                                    "GC of transient table not success (this is not a permanent error). the error : {}",
                                    e
                                );
                            } else {
                                tracing::info!("GC of transient table done");
                            }
                        }
                        metrics_inc_commit_mutation_success();
                        self.heartbeat.shutdown().await?;
                        self.state = State::Finish;
                    }
                    Err(e) if FuseTable::is_error_recoverable(&e, self.transient) => {
                        let table_info = self.table.get_table_info();
                        match self.backoff.next_backoff() {
                            Some(d) => {
                                let name = table_info.name.clone();
                                tracing::debug!(
                                    "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                    d.as_millis(),
                                    name.as_str(),
                                    table_info.ident
                                );
                                common_base::base::tokio::time::sleep(d).await;
                                self.retries += 1;
                                self.state = State::RefreshTable;
                            }
                            None => {
                                // Commit not fulfilled. try to abort the operations.
                                // if it is safe to do so.
                                if FuseTable::no_side_effects_in_meta_store(&e) {
                                    // if we are sure that table state inside metastore has not been
                                    // modified by this operation, abort this operation.
                                    self.state = State::AbortOperation;
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
                let cluster_key_meta = fuse_table.cluster_key_meta.clone();
                self.state = State::GenerateSnapshot {
                    previous,
                    cluster_key_meta,
                };
            }
            State::AbortOperation => {
                metrics_inc_commit_aborts();
                self.heartbeat.shutdown().await?;
                let op = self.abort_operation.clone();
                op.abort(self.ctx.clone(), self.dal.clone()).await?;
                return Err(ErrorCode::StorageOther(
                    "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                ));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
