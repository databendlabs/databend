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

use common_datavalues::DataSchema;
use common_exception::Result;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableIdent;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_planners::InsertIntoPlan;
use common_streams::SendableDataBlockStream;
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
        ctx: Arc<DatabendQueryContext>,
        insert_plan: InsertIntoPlan,
        stream: SendableDataBlockStream,
    ) -> Result<()> {
        // 1. get da
        let da = ctx.get_data_accessor()?;

        // 2. Append blocks to storage
        let segment_info =
            BlockAppender::append_blocks(da.clone(), stream, self.table_info.schema().as_ref())
                .await?;

        // 3. save segment info
        let seg_loc = util::gen_segment_info_location();
        let bytes = serde_json::to_vec(&segment_info)?;
        da.put(&seg_loc, bytes).await?;

        // 4. new snapshot
        let prev_snapshot = self.table_snapshot(ctx.clone()).await?;

        // TODO backoff retry this block
        {
            let new_snapshot = merge_snapshot(
                self.table_info.schema().as_ref(),
                prev_snapshot,
                (segment_info, seg_loc),
            )?;

            // 4.1 save the new snapshot
            let uuid = new_snapshot.snapshot_id;
            let snapshot_loc = util::snapshot_location(uuid.to_simple().to_string().as_str());
            let bytes = serde_json::to_vec(&new_snapshot)?;
            da.put(&snapshot_loc, bytes).await?;

            // 5. commit
            let table_id = insert_plan.tbl_id;
            commit(ctx, table_id, self.table_info.ident.version, snapshot_loc).await?;
        }
        Ok(())
    }
}

fn merge_snapshot(
    schema: &DataSchema,
    pre: Option<TableSnapshot>,
    (seg_info, loc): (SegmentInfo, String),
) -> Result<TableSnapshot> {
    if let Some(s) = pre {
        let mut new_snapshot = s.append_segment(loc);
        let new_stat = util::merge_stats(schema, &new_snapshot.summary, &seg_info.summary)?;
        new_snapshot.summary = new_stat;
        Ok(new_snapshot)
    } else {
        Ok(TableSnapshot {
            snapshot_id: Uuid::new_v4(),
            prev_snapshot_id: None,
            schema: schema.clone(),
            summary: seg_info.summary,
            segments: vec![loc],
        })
    }
}

async fn commit(
    ctx: Arc<DatabendQueryContext>,
    table_id: MetaId,
    table_version: MetaVersion,
    new_snapshot_location: String,
) -> Result<UpsertTableOptionReply> {
    let catalog = ctx.get_catalog();
    catalog
        .upsert_table_option(UpsertTableOptionReq::new(
            &TableIdent {
                table_id,
                version: table_version,
            },
            TBL_OPT_KEY_SNAPSHOT_LOC,
            new_snapshot_location,
        ))
        .await
}
