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

use std::sync::Arc;
use std::time::Instant;

use backoff::backoff::Backoff;
use databend_common_base::base::tokio::time::sleep;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_storages_common_cache::Table;
use databend_storages_common_cache::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;

use super::diff::SegmentsDiff;
use crate::operations::set_backoff;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::deduct_statistics;
use crate::FuseTable;

pub async fn commit_with_backoff(
    ctx: Arc<dyn TableContext>,
    mut req: UpdateMultiTableMetaReq,
) -> Result<()> {
    let catalog = ctx.get_default_catalog()?;
    let mut backoff = set_backoff(None, None, None);
    let mut retries = 0;

    loop {
        let ret = catalog
            .retryable_update_multi_table_meta(req.clone())
            .await?;
        let Err(update_failed_tbls) = ret else {
            return Ok(());
        };
        let Some(duration) = backoff.next_backoff() else {
            return Err(ErrorCode::OCCRetryFailure(retry_too_many_msg(
                retries,
                backoff.start_time,
                update_failed_tbls,
            )));
        };
        sleep(duration).await;
        retries += 1;
        try_rebuild_req(ctx.clone(), &mut req, update_failed_tbls).await?;
    }
}

async fn try_rebuild_req(
    ctx: Arc<dyn TableContext>,
    req: &mut UpdateMultiTableMetaReq,
    update_failed_tbls: Vec<(u64, u64, TableMeta)>,
) -> Result<()> {
    let txn_mgr = ctx.txn_mgr();
    for (tid, seq, table_meta) in update_failed_tbls {
        let latest_table = FuseTable::from_table_meta(tid, seq, table_meta)?;
        let default_cluster_key_id = latest_table.cluster_key_id();
        let latest_snapshot = latest_table.read_table_snapshot().await?;
        let (update_table_meta_req, _) = req
            .update_table_metas
            .iter_mut()
            .find(|(meta, _)| meta.table_id == tid)
            .unwrap();
        let new_table = FuseTable::from_table_meta(
            update_table_meta_req.table_id,
            0,
            update_table_meta_req.new_table_meta.clone(),
        )?;
        let new_snapshot = new_table.read_table_snapshot().await?;
        let base_snapshot_location = txn_mgr.lock().get_base_snapshot_location(tid);
        let base_snapshot = new_table
            .read_table_snapshot_with_location(base_snapshot_location)
            .await?;

        let segments_diff = SegmentsDiff::new(base_snapshot.segments(), new_snapshot.segments());
        let Some(merged_segments) = segments_diff.apply(latest_snapshot.segments().to_vec()) else {
            return Err(ErrorCode::UnresolvableConflict(format!(
                "Unresolvable conflict detected for table {}",
                tid
            )));
        };

        let s = merge_statistics(
            new_snapshot.summary(),
            &latest_snapshot.summary(),
            default_cluster_key_id,
        );
        let merged_summary = deduct_statistics(&s, &base_snapshot.summary());
        let table_meta_timestamps =
            ctx.get_table_meta_timestamps(latest_table.as_ref(), latest_snapshot.clone())?;
        let merged_snapshot = TableSnapshot::try_new(
            Some(seq),
            latest_snapshot.clone(),
            latest_table.schema().as_ref().clone(),
            merged_summary,
            merged_segments,
            latest_snapshot.table_statistics_location(),
            table_meta_timestamps,
        )?;

        // write snapshot
        let dal = latest_table.get_operator();
        let location_generator = &latest_table.meta_location_generator;
        let location = location_generator
            .snapshot_location_from_uuid(&merged_snapshot.snapshot_id, TableSnapshot::VERSION)?;
        dal.write(&location, merged_snapshot.to_bytes()?).await?;

        // build new table meta
        let new_table_meta = FuseTable::build_new_table_meta(
            &latest_table.table_info.meta,
            &location,
            &merged_snapshot,
        )?;
        let table_id = latest_table.table_info.ident.table_id;
        let table_version = latest_table.table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            base_snapshot_location: latest_table.snapshot_loc(),
        };
        *update_table_meta_req = req;
    }
    Ok(())
}

fn retry_too_many_msg(
    retries: u32,
    start_time: Instant,
    update_failed_tbls: Vec<(u64, u64, TableMeta)>,
) -> String {
    format!(
        "Transaction aborted after retries({} times, {} ms). The table_ids that failed to update: {:?}",
        retries,
        Instant::now()
            .duration_since(start_time)
            .as_millis(),
        update_failed_tbls.into_iter().map(|(tid, _, _)| tid).collect::<Vec<_>>()
    )
}
