// Copyright 2022 Datafuse Labs.
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
//

use std::sync::Arc;
use std::time::Instant;

use common_base::base::tokio::sync::mpsc::error::TryRecvError;
use common_base::base::tokio::sync::mpsc::Receiver as TokioReceiver;
use common_base::base::tokio::sync::mpsc::Sender as TokioSender;
use common_cache::Cache;
use common_catalog::catalog::Catalog;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_storages_util::table_option_keys::OPT_KEY_SNAPSHOT_LOCATION;
use futures::channel::oneshot::Sender as OneshotSender;
use opendal::Operator;
use tracing::error;
use tracing::info;
use tracing::log::warn;
use uuid::Uuid;

use super::util;
use crate::io;
use crate::io::TableMetaLocationGenerator;
use crate::operations::write_last_snapshot_hint;
use crate::operations::TableOperationLog;
use crate::statistics;

pub struct CommitAppendTask {
    operation_log: TableOperationLog,
    // TODO use tokio oneshot's?
    call_back_channel: OneshotSender<Result<()>>,
}

impl CommitAppendTask {
    fn ack_ok(self) -> () {
        if self.call_back_channel.send(Ok(())).is_err() {
            // this error is NOT recoverable
            error!("failed to notify tx commit result(success)")
        }
    }
    fn ack_err(self, e: &ErrorCode) {
        if self.call_back_channel.send(Err(e.clone())).is_err() {
            // this error is NOT recoverable
            error!("failed to notify tx commit result(failure)")
        }
    }
}

pub(crate) struct TableCommitter {
    current_snapshot: TableSnapshot,
    rx: TokioReceiver<CommitAppendTask>,
    tx: TokioSender<CommitAppendTask>,
    catalog: Arc<dyn Catalog>,
    operator: Operator,
    table_info: TableInfo,
    location_generator: TableMetaLocationGenerator,
    db_name: String,
    tenant: String,
}

impl TableCommitter {
    pub(crate) fn new() -> Self {
        todo!()
    }

    /// Submits an append operation
    ///
    /// The append operation will be batched if there are concurrent append operations
    pub(crate) async fn submit(&self, log: TableOperationLog, overwrite: bool) -> Result<()> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let task = CommitAppendTask {
            operation_log: log,
            call_back_channel: tx,
        };

        // TODO error code, timeout setting
        self.tx
            .send_timeout(task, std::time::Duration::from_secs(1))
            .await
            .map_err(|e| {
                ErrorCode::OCCRetryFailure(format!("committer is down {}", e.to_string()))
            })?;

        // TODO introduce new error code
        rx.await
            .map_err(|e| ErrorCode::OCCRetryFailure("task aborted"))?
    }

    // the "main routine",  dead loop of receiving and processing tasks
    async fn run(&self, mut rx: TokioReceiver<CommitAppendTask>) -> Result<()> {
        // TODO hook to shutdown handler, e.g. select from two channels
        loop {
            let tasks = self.take_at_most(&mut rx, 10).await?;
            match self.merge_into_new_snapshot(&tasks) {
                Ok(snapshot) => match self.commit_to_meta_server(snapshot).await {
                    Ok(_) => tasks.into_iter().for_each(|task| task.ack_ok()),
                    Err(e) => {
                        warn!("commit failure");
                        tasks.into_iter().for_each(|task| task.ack_err(&e))
                    }
                },
                Err(_) => {
                    // maybe for some kind of errors, we should keep looping
                    break;
                }
            }
        }
        info!("batching committer service quit!");
        Ok(())
    }

    async fn take_at_most(
        &self,
        rx: &mut TokioReceiver<CommitAppendTask>,
        n: usize,
    ) -> Result<Vec<CommitAppendTask>> {
        let mut tasks = Vec::with_capacity(n);
        // blocking read
        match rx.recv().await {
            Some(task) => tasks.push(task),
            None => {
                // closed by peer
                return Err(ErrorCode::SemanticError("lost committer")); // TODO another error code
            }
        }

        // try take more speculatively
        for _ in 0..n - 1 {
            let res = rx.try_recv();
            match res {
                Ok(another) => tasks.push(another),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(ErrorCode::LogicalError("we are done"));
                }
            }
        }

        return Ok(tasks);
    }

    pub async fn commit_to_meta_server(&self, snapshot: TableSnapshot) -> Result<()> {
        let location_generator = &self.location_generator;
        let snapshot_location = location_generator
            .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot.format_version())?;

        // 1. write down snapshot
        let operator = &self.operator;
        let now = Instant::now();
        io::write_meta(&operator, &snapshot_location, &snapshot).await?;
        eprintln!("write_snapshot {:?}", now.elapsed());

        // 2. prepare table meta
        let table_info = &self.table_info;
        let mut new_table_meta = table_info.meta.clone();
        // 2.1 set new snapshot location
        new_table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
            snapshot_location.clone(),
        );
        // remove legacy options
        util::remove_legacy_options(&mut new_table_meta.options);

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

        let catalog = &self.catalog;
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
        };

        // 3. let's roll
        let reply = catalog
            .update_table_meta(&self.tenant, &self.db_name, req)
            .await;
        match reply {
            Ok(_) => {
                if let Some(snapshot_cache) = CacheManager::instance().get_table_snapshot_cache() {
                    let mut cache = snapshot_cache.write();
                    cache.put(snapshot_location.clone(), Arc::new(snapshot));
                }
                // try keep a hit file of last snapshot
                write_last_snapshot_hint(&operator, location_generator, snapshot_location).await;
                Ok(())
            }
            Err(e) => {
                // commit snapshot to meta server failed, try to delete it.
                // "major GC" will collect this, if deletion failure (even after DAL retried)
                let _ = operator.object(&snapshot_location).delete().await;
                Err(e)
            }
        }
    }

    fn merge_into_new_snapshot(&self, tasks: &[CommitAppendTask]) -> Result<TableSnapshot> {
        let combined_log_entries = tasks.iter().map(|task| &task.operation_log).flatten();
        let (segment_paths, statistics) = util::merge_append_operations(combined_log_entries)?;

        let segment_locations = segment_paths
            .into_iter()
            .map(|loc| (loc, SegmentInfo::VERSION))
            .collect();

        let new_snapshot = self.generate_new_snapshot(segment_locations, statistics)?;
        todo!()
    }

    fn generate_new_snapshot(
        &self,
        mut new_segments: Vec<Location>,
        statistics: Statistics,
    ) -> Result<TableSnapshot> {
        let previous = Some(&self.current_snapshot);
        // 1. merge stats with previous snapshot, if any
        let stats = if let Some(snapshot) = &previous {
            let summary = &snapshot.summary;
            statistics::merge_statistics(&statistics, summary)?
        } else {
            statistics
        };

        // TODO
        let prev_version = TableSnapshot::VERSION;
        let prev_snapshot_id = previous.as_ref().map(|v| (v.snapshot_id, prev_version));
        let prev_snapshot_timestamp = previous.as_ref().and_then(|v| v.timestamp);

        // 2. merge segment locations with previous snapshot, if any
        if let Some(snapshot) = &previous {
            let mut segments = snapshot.segments.clone();
            new_segments.append(&mut segments)
        };

        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &prev_snapshot_timestamp,
            prev_snapshot_id,
            self.table_info.meta.schema.as_ref().clone(), // assuming append only operations
            stats,
            new_segments,
            self.current_snapshot.cluster_key_meta.clone(),
        );
        Ok(new_snapshot)
    }
}
