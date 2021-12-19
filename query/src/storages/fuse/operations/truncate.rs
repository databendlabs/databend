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
use common_meta_types::UpsertTableOptionReq;
use common_planners::TruncateTablePlan;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::fuse::io;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;

impl FuseTable {
    #[inline]
    pub async fn do_truncate(
        &self,
        ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        if let Some(prev_snapshot) = self.table_snapshot(ctx.as_ref()).await? {
            let prev_id = prev_snapshot.snapshot_id;
            let mut new_snapshot = prev_snapshot;
            new_snapshot.segments = vec![];
            new_snapshot.prev_snapshot_id = Some(prev_id);
            new_snapshot.summary = Default::default();
            new_snapshot.snapshot_id = Uuid::new_v4();
            let new_snapshot_loc = io::snapshot_location(&new_snapshot.snapshot_id);
            let da = ctx.get_data_accessor()?;
            let bytes = serde_json::to_vec(&new_snapshot)?;
            da.put(&new_snapshot_loc, bytes).await?;

            let keep_last_snapshot = false;
            let _ = self
                .do_truncate_history(ctx.clone(), keep_last_snapshot)
                .await;
            ctx.get_catalog()
                .upsert_table_option(UpsertTableOptionReq::new(
                    &self.table_info.ident,
                    TBL_OPT_KEY_SNAPSHOT_LOC,
                    new_snapshot_loc,
                ))
                .await?;
        }

        Ok(())
    }
}
