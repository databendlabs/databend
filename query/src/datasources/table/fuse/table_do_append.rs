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

use common_catalog::IOContext;
use common_catalog::TableIOContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use uuid::Uuid;

use crate::datasources::table::fuse::util;
use crate::datasources::table::fuse::BlockAppender;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::TableSnapshot;

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
        let segment_info = BlockAppender::append_blocks(da.clone(), block_stream).await?;

        let seg_loc = {
            let uuid = Uuid::new_v4().to_simple().to_string();
            util::segment_info_location(&uuid)
        };

        {
            let bytes = serde_json::to_vec(&segment_info)?;
            da.put(&seg_loc, bytes).await?;
        }

        // 3. new snapshot
        let tbl_snapshot = self
            .table_snapshot(io_ctx)?
            .unwrap_or_else(TableSnapshot::new);
        let _snapshot_id = tbl_snapshot.snapshot_id;
        let new_snapshot = tbl_snapshot.append_segment(seg_loc);
        let _new_snapshot_id = new_snapshot.snapshot_id;

        {
            let uuid = Uuid::new_v4().to_simple().to_string();
            let snapshot_loc = util::snapshot_location(&uuid);

            let bytes = serde_json::to_vec(&new_snapshot)?;
            da.put(&snapshot_loc, bytes).await?;
        }

        // 4. commit
        let _table_id = insert_plan.tbl_id;
        // TODO simple retry strategy
        // self.meta_client
        //     .commit_table(
        //         table_id,
        //         snapshot_id.to_simple().to_string(),
        //         new_snapshot_id.to_simple().to_string(),
        //     )
        //     .await?;
        Ok(())
    }
}
