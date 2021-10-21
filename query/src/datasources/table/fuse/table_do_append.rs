//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use common_context::IOContext;
use common_context::TableIOContext;
use common_dal::DataAccessor;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_planners::InsertIntoPlan;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::datasources::table::fuse::util;
use crate::datasources::table::fuse::util::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::datasources::table::fuse::BlockAppender;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::SegmentInfo;
use crate::datasources::table::fuse::TableSnapshot;
use crate::sessions::DatabendQueryContext;

impl FuseTable {
    #[inline]
    pub async fn do_append(
        &self,
        io_ctx: Arc<TableIOContext>,
        insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        // 1. take out input stream from plan
        //    Assumes that, insert_interpreter has properly batched data blocks
        let block_stream = {
            match insert_plan.input_stream.lock().take() {
                Some(s) => s,
                None => return Err(ErrorCode::EmptyData("input stream consumed")),
            }
        };

        let da = io_ctx.get_data_accessor()?;

        // 2. Append blocks to storage
        let segment_info =
            BlockAppender::append_blocks(da.clone(), block_stream, self.table_info.schema.as_ref())
                .await?;

        // 3. save segment info
        let seg_loc = util::gen_segment_info_location();
        let bytes = serde_json::to_vec(&segment_info)?;
        da.put(&seg_loc, bytes).await?;

        // 4. new snapshot

        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");
        let mut version = self.table_info.version;

        // Here we use the backoff only, `backoff::future::retry` might not that handy here (passing around the mut version)
        let mut backoff = ExponentialBackoff::default();
        loop {
            let prev_snapshot = self.table_snapshot_with_version(version, io_ctx.as_ref())?;
            let new_snapshot = merge_snapshot(
                self.table_info.schema.as_ref(),
                &prev_snapshot,
                (&segment_info, &seg_loc),
            )?;
            let snapshot_loc = save_snapshot(&new_snapshot, da.as_ref()).await?;

            // 5. commit
            let table_id = insert_plan.tbl_id;
            let commit_res = update_snapshot_location(&io_ctx, table_id, version, snapshot_loc);
            if let Err(err) = commit_res {
                if err.code() == ErrorCode::TableVersionMissMatch("").code() {
                    match backoff.next_backoff() {
                        Some(duration) => {
                            common_base::tokio::time::sleep(duration).await;
                            let table = ctx.get_catalog().get_table(
                                self.table_info.db.as_str(),
                                self.table_info.name.as_str(),
                            )?;
                            version = table.get_table_info().version;
                            continue;
                        }
                        None => break Err(ErrorCode::CommitTableError("commit table failure")),
                    }
                }
            }
            break Ok(());
        }
    }
}

fn merge_snapshot(
    schema: &DataSchema,
    pre: &Option<TableSnapshot>,
    (seg_info, loc): (&SegmentInfo, &String),
) -> Result<TableSnapshot> {
    if let Some(s) = pre {
        let s = s.clone();
        let mut new_snapshot = s.append_segment(loc.clone());
        let new_stat = util::merge_stats(schema, &new_snapshot.summary, &seg_info.summary)?;
        new_snapshot.summary = new_stat;
        Ok(new_snapshot)
    } else {
        Ok(TableSnapshot {
            snapshot_id: Uuid::new_v4(),
            prev_snapshot_id: None,
            schema: schema.clone(),
            summary: seg_info.summary.clone(),
            segments: vec![loc.clone()],
        })
    }
}

fn update_snapshot_location(
    io_ctx: &TableIOContext,
    table_id: MetaId,
    table_version: MetaVersion,
    new_snapshot_location: String,
) -> Result<()> {
    let ctx: Arc<DatabendQueryContext> = io_ctx
        .get_user_data()?
        .expect("DatabendQueryContext should not be None");
    let catalog = ctx.get_catalog();
    catalog.upsert_table_option(
        table_id,
        table_version,
        TBL_OPT_KEY_SNAPSHOT_LOC.to_string(),
        new_snapshot_location,
    )
}

async fn save_snapshot(new_snapshot: &TableSnapshot, da: &dyn DataAccessor) -> Result<String> {
    let uuid = new_snapshot.snapshot_id;
    let snapshot_loc = util::snapshot_location(uuid.to_simple().to_string().as_str());
    let bytes = serde_json::to_vec(&new_snapshot).map_err(ErrorCode::from_std_error)?;
    da.put(&snapshot_loc, bytes).await?;
    Ok(snapshot_loc)
}
