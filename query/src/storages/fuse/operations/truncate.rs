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

use common_exception::Result;
use common_meta_types::MatchSeq;
use common_meta_types::TableStatistics;
use common_meta_types::UpdateTableMetaReq;
use common_planners::TruncateTablePlan;
use uuid::Uuid;

use crate::sessions::QueryContext;
use crate::sql::OPT_KEY_SNAPSHOT_LOCATION;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::meta::Versioned;
use crate::storages::fuse::FuseTable;

impl FuseTable {
    #[inline]
    pub async fn do_truncate(&self, ctx: Arc<QueryContext>, plan: TruncateTablePlan) -> Result<()> {
        if let Some(prev_snapshot) = self.read_table_snapshot(ctx.as_ref()).await? {
            let prev_id = prev_snapshot.snapshot_id;

            let new_snapshot = TableSnapshot::new(
                Uuid::new_v4(),
                Some((prev_id, prev_snapshot.format_version())),
                prev_snapshot.schema.clone(),
                Default::default(),
                vec![],
            );
            let loc = self.meta_location_generator();
            let new_snapshot_loc =
                loc.snapshot_location_from_uuid(&new_snapshot.snapshot_id, TableSnapshot::VERSION)?;
            let operator = ctx.get_storage_operator()?;
            let bytes = serde_json::to_vec(&new_snapshot)?;
            operator.object(&new_snapshot_loc).write(bytes).await?;

            if plan.purge {
                let keep_last_snapshot = false;
                self.do_optimize(ctx.clone(), keep_last_snapshot).await?
            }

            let mut new_table_meta = self.table_info.meta.clone();
            // update snapshot location
            new_table_meta
                .options
                .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), new_snapshot_loc);
            // update table statistics
            new_table_meta.statistics = Some(TableStatistics::default());

            let table_id = self.table_info.ident.table_id;
            let table_version = self.table_info.ident.seq;
            ctx.get_catalog(&plan.catalog)?
                .update_table_meta(UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta,
                })
                .await?;
        }

        Ok(())
    }
}
