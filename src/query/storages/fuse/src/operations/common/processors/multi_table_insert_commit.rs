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
use std::time::Instant;

use async_trait::async_trait;
use backoff::backoff::Backoff;
use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_meta_types::MatchSeq;
use databend_common_pipeline::sinks::AsyncSink;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use log::debug;
use log::error;
use log::info;

use crate::FuseTable;
use crate::operations::AppendGenerator;
use crate::operations::CommitMeta;
use crate::operations::SnapshotGenerator;
use crate::operations::TransformMergeCommitMeta;
use crate::operations::set_backoff;
use crate::operations::set_compaction_num_block_hint;
use crate::statistics::gen_table_statistics;

pub struct CommitMultiTableInsert {
    commit_metas: HashMap<u64, CommitMeta>,
    tables: HashMap<u64, Arc<dyn Table>>,
    ctx: Arc<dyn TableContext>,
    overwrite: bool,
    update_stream_meta: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
    catalog: Arc<dyn Catalog>,
    table_meta_timestamps: HashMap<u64, TableMetaTimestamps>,
}

impl CommitMultiTableInsert {
    pub fn create(
        tables: HashMap<u64, Arc<dyn Table>>,
        ctx: Arc<dyn TableContext>,
        overwrite: bool,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
        catalog: Arc<dyn Catalog>,
        table_meta_timestamps: HashMap<u64, TableMetaTimestamps>,
    ) -> Self {
        Self {
            commit_metas: Default::default(),
            tables,
            ctx,
            overwrite,
            update_stream_meta,
            deduplicated_label,
            catalog,
            table_meta_timestamps,
        }
    }
}

#[async_trait]
impl AsyncSink for CommitMultiTableInsert {
    const NAME: &'static str = "CommitMultiTableInsert";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        let mut update_table_metas = Vec::with_capacity(self.commit_metas.len());
        let mut update_temp_tables = Vec::with_capacity(self.commit_metas.len());
        let insert_rows = {
            let stats = self.ctx.get_multi_table_insert_status();
            let status = stats.lock();
            status.insert_rows.clone()
        };

        let mut table_write_states = HashMap::with_capacity(self.commit_metas.len());
        for (ref_id, commit_meta) in std::mem::take(&mut self.commit_metas).into_iter() {
            let table = self.tables.remove(&ref_id).unwrap();
            if table.is_temp() {
                update_temp_tables.push(UpdateTempTableReq {
                    table_id: ref_id,
                    new_table_meta: table.get_table_info().meta.clone(),
                    copied_files: Default::default(),
                    desc: table.get_table_info().desc.clone(),
                });
                continue;
            }

            // Get or create TableWriteState
            let state = table_write_states
                .entry(table.get_table_id())
                .or_insert_with(|| TableWriteState {
                    table: table.clone(),
                    builds: HashMap::new(),
                });

            // Create snapshot generator
            let mut snapshot_generator = AppendGenerator::new(self.ctx.clone(), self.overwrite);
            snapshot_generator.set_conflict_resolve_context(commit_meta.conflict_resolve_context);
            let table_meta_timestamps = self.table_meta_timestamps.get(&ref_id).unwrap();

            // Determine if this is main or branch
            let (branch_name, branch_id) = table
                .get_branch_info()
                .map(|b| (b.branch_name().to_string(), b.branch_id()))
                .unzip();

            state.builds.insert(branch_name, SnapshotBuild {
                branch_id,
                hll: commit_meta.hll,
                snapshot_generator,
                table_meta_timestamps: *table_meta_timestamps,
                schema: table.schema(),
                cluster_key_meta: table.cluster_key_meta(),
                insert_rows: insert_rows.get(&ref_id).cloned().unwrap_or_default(),
            });
        }

        for write_state in table_write_states.values() {
            update_table_metas.push(write_state.build_update_table_meta_req().await?);
        }

        let mut backoff = set_backoff(None, None, None);
        let mut retries = 0;

        loop {
            let update_multi_table_meta_req = UpdateMultiTableMetaReq {
                update_table_metas: update_table_metas.clone(),
                copied_files: vec![],
                update_stream_metas: self.update_stream_meta.clone(),
                deduplicated_labels: self.deduplicated_label.clone().into_iter().collect(),
                update_temp_tables: std::mem::take(&mut update_temp_tables),
            };

            let update_meta_result = match self
                .catalog
                .retryable_update_multi_table_meta(update_multi_table_meta_req)
                .await
            {
                Ok(ret) => ret,
                Err(e) => {
                    // other errors may occur, especially the version mismatch of streams,
                    // let's log it here for the convenience of diagnostics
                    error!(
                        "Non-recoverable fault occurred during updating tables. {}",
                        e
                    );
                    return Err(e);
                }
            };

            let Err(update_failed_tbls) = update_meta_result else {
                let table_descriptions = table_write_states
                    .values()
                    .map(|state| {
                        let table_info = state.table.get_table_info();
                        (&table_info.desc, &table_info.ident, &table_info.meta.engine)
                    })
                    .collect::<Vec<_>>();
                let stream_descriptions = self
                    .update_stream_meta
                    .iter()
                    .map(|s| (s.stream_id, s.seq, "stream"))
                    .collect::<Vec<_>>();
                info!(
                    "update tables success (auto commit), tables updated {:?}, streams updated {:?}",
                    table_descriptions, stream_descriptions
                );

                return Ok(());
            };
            let update_failed_tbl_descriptions: Vec<_> = update_failed_tbls
                .iter()
                .map(|(tid, seq, meta)| {
                    let tbl_info = table_write_states.get(tid).unwrap().table.get_table_info();
                    (&tbl_info.desc, (tid, seq), &meta.engine)
                })
                .collect();
            match backoff.next_backoff() {
                Some(duration) => {
                    retries += 1;

                    debug!(
                        "Failed(temporarily) to update tables: {:?}, the commit process of multi-table insert will be retried after {} ms, retrying {} times",
                        update_failed_tbl_descriptions,
                        duration.as_millis(),
                        retries,
                    );
                    tokio::time::sleep(duration).await;
                    for (tid, seq, meta) in update_failed_tbls {
                        let state = table_write_states.get_mut(&tid).unwrap();
                        state.table = state
                            .table
                            .refresh_with_seq_meta(self.ctx.as_ref(), seq, meta)
                            .await?;
                        for (req, _) in update_table_metas.iter_mut() {
                            if req.table_id == tid {
                                *req = state.build_update_table_meta_req().await?.0;
                                break;
                            }
                        }
                    }
                }
                None => {
                    let err_msg = format!(
                        "Can not fulfill the tx after retries({} times, {} ms), aborted. updated tables {:?}",
                        retries,
                        Instant::now()
                            .duration_since(backoff.start_time)
                            .as_millis(),
                        update_failed_tbl_descriptions,
                    );
                    error!("{}", err_msg);
                    return Err(ErrorCode::OCCRetryFailure(err_msg));
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let input_meta = data_block
            .get_meta()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;

        let meta = CommitMeta::downcast_from(input_meta)
            .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?;
        match self.commit_metas.get_mut(&meta.table_ref_id) {
            Some(m) => {
                let table = self.tables.get(&meta.table_ref_id).unwrap();
                let table = FuseTable::try_from_table(table.as_ref()).unwrap();
                *m = TransformMergeCommitMeta::merge_commit_meta(
                    m.clone(),
                    meta,
                    table.cluster_key_id(),
                );
            }
            None => {
                self.commit_metas.insert(meta.table_ref_id, meta);
            }
        }
        Ok(false)
    }
}

/// Contains information needed to build a snapshot for a table or branch.
struct SnapshotBuild {
    branch_id: Option<u64>,

    /// HyperLogLog structure for NDV/statistics.
    hll: BlockHLL,

    /// Snapshot generator for creating table/branch snapshots.
    snapshot_generator: AppendGenerator,

    /// Timestamp info for table metadata.
    table_meta_timestamps: TableMetaTimestamps,

    schema: Arc<TableSchema>,

    cluster_key_meta: Option<ClusterKey>,

    /// Number of rows inserted.
    insert_rows: u64,
}

/// Represents the snapshot build state for a table, including main and branch snapshots.
struct TableWriteState {
    /// The table object.
    table: Arc<dyn Table>,

    /// Mapping from branch_name to its snapshot build state.
    builds: HashMap<Option<String>, SnapshotBuild>,
}

impl TableWriteState {
    /// Builds snapshots for all branches and the main table, returning the commit input.
    #[async_backtrace::framed]
    async fn build_update_table_meta_req(&self) -> Result<(UpdateTableMetaReq, TableInfo)> {
        let fuse_table = FuseTable::try_from_table(self.table.as_ref())?;
        let table_info = fuse_table.get_table_info();
        let options = table_info.options();
        let dal = fuse_table.get_operator();
        let location_generator = &fuse_table.meta_location_generator;

        let mut base_snapshot_locations = HashMap::with_capacity(self.builds.len());
        let mut new_table_meta = table_info.meta.clone();
        for (branch_name, build) in self.builds.iter() {
            let ctx = build.snapshot_generator.ctx.as_ref();
            let previous_location = if let Some(branch_name) = branch_name {
                let Some(snapshot_ref) = table_info.meta.refs.get(branch_name) else {
                    return Err(ErrorCode::UnknownReference(format!(
                        "Branch '{}' was dropped on table {}",
                        branch_name, table_info.desc
                    )));
                };
                if Some(snapshot_ref.id) != build.branch_id {
                    return Err(ErrorCode::UnknownReference(format!(
                        "Branch '{}' was changed on table {}",
                        branch_name, table_info.desc
                    )));
                }
                Some(snapshot_ref.loc.clone())
            } else {
                options
                    .get(OPT_KEY_SNAPSHOT_LOCATION)
                    .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))
                    .cloned()
            };
            base_snapshot_locations.insert(build.branch_id, previous_location.clone());
            let previous =
                FuseTable::read_snapshot_with_opt_location(dal.clone(), previous_location).await?;
            let table_stats_gen = FuseTable::generate_table_stats(
                dal.clone(),
                location_generator,
                &previous,
                build.branch_id,
                &build.hll,
                build.insert_rows,
            )
            .await?;
            let snapshot = build.snapshot_generator.generate_new_snapshot(
                &table_info.ident,
                build.schema.as_ref(),
                build.cluster_key_meta.clone(),
                previous,
                ctx.txn_mgr(),
                build.table_meta_timestamps,
                table_stats_gen,
            )?;
            snapshot.ensure_segments_unique()?;

            // Write snapshot to storage
            let location = location_generator.gen_snapshot_location(
                build.branch_id,
                &snapshot.snapshot_id,
                TableSnapshot::VERSION,
            )?;
            dal.write(&location, snapshot.to_bytes()?).await?;

            if let Some(branch_name) = branch_name {
                let branch_ref = new_table_meta.refs.get_mut(branch_name).unwrap();
                branch_ref.loc = location;
            } else {
                set_compaction_num_block_hint(ctx, table_info.name.as_str(), &snapshot.summary);
                new_table_meta
                    .options
                    .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), location.to_owned());
                new_table_meta.options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
                new_table_meta.statistics = gen_table_statistics(&snapshot);
            }
        }
        new_table_meta.updated_on = Utc::now();
        let req = UpdateTableMetaReq {
            table_id: table_info.ident.table_id,
            seq: MatchSeq::Exact(table_info.ident.seq),
            new_table_meta,
            base_snapshot_locations,
            lvt_check: None,
        };
        Ok((req, table_info.clone()))
    }
}
