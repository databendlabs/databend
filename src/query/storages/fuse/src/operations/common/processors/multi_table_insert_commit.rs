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
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_pipeline::sinks::AsyncSink;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
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

pub struct CommitMultiTableInsert {
    commit_metas: HashMap<u64, CommitMeta>,
    tables: HashMap<u64, Arc<dyn Table>>,
    ctx: Arc<dyn TableContext>,
    overwrite: bool,
    update_stream_meta: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
    catalog: Arc<dyn Catalog>,
    table_meta_timestampss: HashMap<u64, TableMetaTimestamps>,
}

impl CommitMultiTableInsert {
    pub fn create(
        tables: HashMap<u64, Arc<dyn Table>>,
        ctx: Arc<dyn TableContext>,
        overwrite: bool,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
        catalog: Arc<dyn Catalog>,
        table_meta_timestampss: HashMap<u64, TableMetaTimestamps>,
    ) -> Self {
        Self {
            commit_metas: Default::default(),
            tables,
            ctx,
            overwrite,
            update_stream_meta,
            deduplicated_label,
            catalog,
            table_meta_timestampss,
        }
    }
}

#[async_trait]
impl AsyncSink for CommitMultiTableInsert {
    const NAME: &'static str = "CommitMultiTableInsert";

    const CALL_ON_FINISH_ON_ERROR: bool = false;

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        let mut update_table_metas = Vec::with_capacity(self.commit_metas.len());
        let mut update_temp_tables = Vec::with_capacity(self.commit_metas.len());
        let mut snapshot_generators = HashMap::with_capacity(self.commit_metas.len());
        let mut hlls = HashMap::with_capacity(self.commit_metas.len());
        let insert_rows = {
            let stats = self.ctx.mutation_state().multi_table_insert_status();
            let status = stats.lock().unwrap();
            status.insert_rows.clone()
        };
        for (table_id, commit_meta) in std::mem::take(&mut self.commit_metas).into_iter() {
            // generate snapshot
            let mut snapshot_generator = AppendGenerator::new(self.ctx.clone(), self.overwrite);
            snapshot_generator.set_conflict_resolve_context(commit_meta.conflict_resolve_context);
            let table = self.tables.get(&table_id).unwrap();
            if table.is_temp() {
                update_temp_tables.push(
                    build_update_temp_table_req(
                        table.as_ref(),
                        &snapshot_generator,
                        self.ctx.txn_mgr(),
                        *self.table_meta_timestampss.get(&table_id).unwrap(),
                        &commit_meta.hll,
                        insert_rows.get(&table_id).cloned().unwrap_or_default(),
                    )
                    .await?,
                );
            } else {
                update_table_metas.push((
                    build_update_table_meta_req(
                        table.as_ref(),
                        &snapshot_generator,
                        self.ctx.txn_mgr(),
                        *self.table_meta_timestampss.get(&table.get_id()).unwrap(),
                        &commit_meta.hll,
                        insert_rows.get(&table_id).cloned().unwrap_or_default(),
                    )
                    .await?,
                    table.get_table_info().clone(),
                ));
            }
            snapshot_generators.insert(table_id, snapshot_generator);
            hlls.insert(table_id, commit_meta.hll);
        }

        let mut backoff = set_backoff(None, None, None);
        let mut retries = 0;

        loop {
            let update_multi_table_meta_req = build_non_temp_update_multi_table_meta_req(
                update_table_metas.clone(),
                self.update_stream_meta.clone(),
                self.deduplicated_label.clone(),
            );

            let update_meta_result = if update_multi_table_meta_req.is_empty() {
                Ok(Default::default())
            } else {
                match self
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
                }
            };

            let Err(update_failed_tbls) = update_meta_result else {
                if !update_temp_tables.is_empty() {
                    self.catalog
                        .update_multi_table_meta(build_temp_update_multi_table_meta_req(
                            std::mem::take(&mut update_temp_tables),
                        ))
                        .await?;
                }

                let table_descriptions = self
                    .tables
                    .values()
                    .map(|tbl| {
                        let table_info = tbl.get_table_info();
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
                    let tbl_info = self.tables.get(tid).unwrap().get_table_info();
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
                        let table = self.tables.get_mut(&tid).unwrap();
                        *table = table
                            .refresh_with_seq_meta(self.ctx.as_ref(), seq, meta)
                            .await?;
                        for (req, _) in update_table_metas.iter_mut() {
                            if req.table_id == tid {
                                *req = build_update_table_meta_req(
                                    table.as_ref(),
                                    snapshot_generators.get(&tid).unwrap(),
                                    self.ctx.txn_mgr(),
                                    *self.table_meta_timestampss.get(&tid).unwrap(),
                                    hlls.get(&tid).unwrap(),
                                    insert_rows.get(&tid).cloned().unwrap_or_default(),
                                )
                                .await?;
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
        match self.commit_metas.get_mut(&meta.table_id) {
            Some(m) => {
                let table = self.tables.get(&meta.table_id).unwrap();
                let table = FuseTable::try_from_table(table.as_ref()).unwrap();
                *m = TransformMergeCommitMeta::merge_commit_meta(
                    m.clone(),
                    meta,
                    table.cluster_key_id(),
                )?;
            }
            None => {
                self.commit_metas.insert(meta.table_id, meta);
            }
        }
        Ok(false)
    }
}

fn build_non_temp_update_multi_table_meta_req(
    update_table_metas: Vec<(UpdateTableMetaReq, TableInfo)>,
    update_stream_metas: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
) -> UpdateMultiTableMetaReq {
    UpdateMultiTableMetaReq {
        update_table_metas,
        copied_files: vec![],
        update_stream_metas,
        deduplicated_labels: deduplicated_label.into_iter().collect(),
        update_temp_tables: vec![],
    }
}

fn build_temp_update_multi_table_meta_req(
    update_temp_tables: Vec<UpdateTempTableReq>,
) -> UpdateMultiTableMetaReq {
    UpdateMultiTableMetaReq {
        update_temp_tables,
        ..Default::default()
    }
}

async fn build_update_temp_table_req(
    table: &dyn Table,
    snapshot_generator: &AppendGenerator,
    txn_mgr: TxnManagerRef,
    table_meta_timestamps: TableMetaTimestamps,
    insert_hll: &BlockHLL,
    insert_rows: u64,
) -> Result<UpdateTempTableReq> {
    let table_info = table.get_table_info();
    let new_table_meta = write_new_snapshot_and_build_table_meta(
        table,
        snapshot_generator,
        txn_mgr,
        table_meta_timestamps,
        insert_hll,
        insert_rows,
    )
    .await?;

    Ok(UpdateTempTableReq {
        table_id: table_info.ident.table_id,
        new_table_meta,
        copied_files: Default::default(),
        desc: table_info.desc.clone(),
    })
}

async fn build_update_table_meta_req(
    table: &dyn Table,
    snapshot_generator: &AppendGenerator,
    txn_mgr: TxnManagerRef,
    table_meta_timestamps: TableMetaTimestamps,
    insert_hll: &BlockHLL,
    insert_rows: u64,
) -> Result<UpdateTableMetaReq> {
    let fuse_table = FuseTable::try_from_table(table)?;
    let new_table_meta = write_new_snapshot_and_build_table_meta(
        table,
        snapshot_generator,
        txn_mgr,
        table_meta_timestamps,
        insert_hll,
        insert_rows,
    )
    .await?;
    let table_id = fuse_table.table_info.ident.table_id;
    let table_version = fuse_table.table_info.ident.seq;

    let req = UpdateTableMetaReq {
        table_id,
        seq: MatchSeq::Exact(table_version),
        new_table_meta,
        base_snapshot_location: fuse_table.snapshot_loc(),
        lvt_check: None,
    };
    Ok(req)
}

async fn write_new_snapshot_and_build_table_meta(
    table: &dyn Table,
    snapshot_generator: &AppendGenerator,
    txn_mgr: TxnManagerRef,
    table_meta_timestamps: TableMetaTimestamps,
    insert_hll: &BlockHLL,
    insert_rows: u64,
) -> Result<TableMeta> {
    let fuse_table = FuseTable::try_from_table(table)?;
    let previous = fuse_table.read_table_snapshot().await?;
    let table_stats_gen = fuse_table
        .generate_table_stats(&previous, insert_hll, insert_rows)
        .await?;
    let table_info = table.get_table_info();
    let snapshot = snapshot_generator.generate_new_snapshot(
        table_info,
        fuse_table.cluster_key_meta(),
        fuse_table.cluster_type(),
        previous,
        txn_mgr,
        table_meta_timestamps,
        table_stats_gen,
    )?;
    snapshot.ensure_segments_unique()?;
    set_compaction_num_block_hint(
        snapshot_generator.ctx.as_ref(),
        table_info.name.as_str(),
        &snapshot.summary,
    );

    let dal = fuse_table.get_operator();
    let location_generator = &fuse_table.meta_location_generator;
    let location =
        location_generator.gen_snapshot_location(&snapshot.snapshot_id, TableSnapshot::VERSION)?;
    dal.write(&location, snapshot.to_bytes()?).await?;

    Ok(FuseTable::build_new_table_meta(
        &fuse_table.table_info.meta,
        &location,
        &snapshot,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_temp_update_req_does_not_carry_temp_table_updates() {
        let req =
            build_non_temp_update_multi_table_meta_req(vec![], vec![], Some("label".to_string()));

        assert!(req.update_table_metas.is_empty());
        assert!(req.copied_files.is_empty());
        assert!(req.update_stream_metas.is_empty());
        assert_eq!(req.deduplicated_labels, vec!["label".to_string()]);
        assert!(req.update_temp_tables.is_empty());
    }

    #[test]
    fn temp_update_req_only_carries_temp_table_updates() {
        let temp_req = UpdateTempTableReq {
            table_id: 1,
            desc: "default.tmp".to_string(),
            new_table_meta: TableMeta::default(),
            copied_files: Default::default(),
        };
        let req = build_temp_update_multi_table_meta_req(vec![temp_req]);

        assert!(req.update_table_metas.is_empty());
        assert!(req.copied_files.is_empty());
        assert!(req.update_stream_metas.is_empty());
        assert!(req.deduplicated_labels.is_empty());
        assert_eq!(req.update_temp_tables.len(), 1);
    }
}
