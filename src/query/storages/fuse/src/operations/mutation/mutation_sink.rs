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

use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoPtr;
use opendal::Operator;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use tracing::info;

use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_inc_commit_mutation_resolvable_conflict;
use crate::metrics::metrics_inc_commit_mutation_retry;
use crate::metrics::metrics_inc_commit_mutation_success;
use crate::metrics::metrics_inc_commit_mutation_unresolvable_conflict;
use crate::operations::commit::Conflict;
use crate::operations::commit::MutatorConflictDetector;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::MutationSinkMeta;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::reducers::merge_statistics_mut;
use crate::FuseTable;

const MAX_RETRIES: u64 = 10;

enum State {
    None,
    ReadMeta(BlockMetaInfoPtr),
    TryCommit(TableSnapshot),
    RefreshTable,
    DetectConflict(Arc<TableSnapshot>),
    MergeSegments(Vec<Location>),
    AbortOperation,
    Finish,
}

// Gathers all the segments and commits to the meta server.
pub struct MutationSink {
    state: State,

    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    table: Arc<dyn Table>,
    base_snapshot: Arc<TableSnapshot>,
    // locations all the merged segments.
    merged_segments: Vec<Location>,
    // summarised statistics of all the merged segments.
    merged_statistics: Statistics,
    abort_operation: AbortOperation,

    retries: u64,

    input: Arc<InputPort>,
}

impl MutationSink {
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        input: Arc<InputPort>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MutationSink {
            state: State::None,
            ctx,
            dal: table.get_operator(),
            location_gen: table.meta_location_generator.clone(),
            table: Arc::new(table.clone()),
            base_snapshot,
            merged_segments: vec![],
            merged_statistics: Statistics::default(),
            abort_operation: AbortOperation::default(),
            retries: 0,
            input,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for MutationSink {
    fn name(&self) -> String {
        "MutationSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(&self.state, State::DetectConflict(_)) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::MergeSegments(_)
                | State::TryCommit(_)
                | State::RefreshTable
                | State::AbortOperation
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

        let input_meta = self
            .input
            .pull_data()
            .unwrap()?
            .get_meta()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
        self.state = State::ReadMeta(input_meta);
        self.input.finish();
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::ReadMeta(input_meta) => {
                // Status
                {
                    let status = "mutation: begin try to commit";
                    self.ctx.set_status_info(status);
                    info!(status);
                }

                let meta = MutationSinkMeta::from_meta(&input_meta)?;

                self.merged_segments = meta.segments.clone();
                self.merged_statistics = meta.summary.clone();
                self.abort_operation = meta.abort_operation.clone();

                let mut new_snapshot = TableSnapshot::from_previous(&self.base_snapshot);
                new_snapshot.segments = self.merged_segments.clone();
                new_snapshot.summary = self.merged_statistics.clone();
                self.state = State::TryCommit(new_snapshot);
            }
            State::DetectConflict(latest_snapshot) => {
                // Check if there is only insertion during the operation.
                match MutatorConflictDetector::detect_conflicts(
                    self.base_snapshot.as_ref(),
                    latest_snapshot.as_ref(),
                ) {
                    Conflict::Unresolvable => {
                        metrics_inc_commit_mutation_unresolvable_conflict();
                        self.state = State::AbortOperation;
                    }
                    Conflict::ResolvableAppend(range_of_newly_append) => {
                        tracing::info!("resolvable conflicts detected");
                        metrics_inc_commit_mutation_resolvable_conflict();

                        self.retries += 1;
                        metrics_inc_commit_mutation_retry();

                        self.state = State::MergeSegments(
                            latest_snapshot.segments[range_of_newly_append].to_owned(),
                        );
                        self.base_snapshot = latest_snapshot;
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
            State::TryCommit(new_snapshot) => {
                let table_info = self.table.get_table_info();
                match FuseTable::commit_to_meta_server(
                    self.ctx.as_ref(),
                    table_info,
                    &self.location_gen,
                    new_snapshot,
                    None,
                    &None,
                    &self.dal,
                )
                .await
                {
                    Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
                        if self.retries < MAX_RETRIES {
                            self.state = State::RefreshTable;
                        } else {
                            tracing::error!(
                                "commit mutation failed after {} retries",
                                self.retries
                            );
                            self.state = State::AbortOperation;
                        }
                    }
                    Err(e) => return Err(e),
                    Ok(_) => {
                        metrics_inc_commit_mutation_success();
                        self.state = State::Finish;
                    }
                };
            }
            State::RefreshTable => {
                self.table = self.table.refresh(self.ctx.as_ref()).await?;
                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?.to_owned();
                let latest_snapshot = fuse_table.read_table_snapshot().await?.ok_or_else(|| {
                    ErrorCode::Internal(
                        "mutation meets empty snapshot during conflict reconciliation",
                    )
                })?;
                self.state = State::DetectConflict(latest_snapshot);
            }
            State::MergeSegments(appended_segments) => {
                let mut new_snapshot = TableSnapshot::from_previous(&self.base_snapshot);
                if !appended_segments.is_empty() {
                    self.merged_segments = appended_segments
                        .iter()
                        .chain(self.merged_segments.iter())
                        .cloned()
                        .collect();
                    let segments_io =
                        SegmentsIO::create(self.ctx.clone(), self.dal.clone(), self.table.schema());
                    let append_segment_infos =
                        segments_io.read_segments(&appended_segments, true).await?;
                    for result in append_segment_infos.into_iter() {
                        let appended_segment = result?;
                        merge_statistics_mut(
                            &mut self.merged_statistics,
                            &appended_segment.summary,
                        )?;
                    }
                }
                new_snapshot.segments = self.merged_segments.clone();
                new_snapshot.summary = self.merged_statistics.clone();
                self.state = State::TryCommit(new_snapshot);
            }
            State::AbortOperation => {
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
