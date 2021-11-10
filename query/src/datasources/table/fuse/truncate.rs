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

use common_context::IOContext;
use common_context::TableIOContext;
use common_exception::Result;
use common_planners::TruncateTablePlan;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::datasources::table::fuse::util;
use crate::datasources::table::fuse::util::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::datasources::table::fuse::FuseTable;
use crate::sessions::DatabendQueryContext;

impl FuseTable {
    #[inline]
    pub async fn do_truncate(
        &self,
        io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        if let Some(prev_snapshot) = self.table_snapshot(&io_ctx).await? {
            let prev_id = prev_snapshot.snapshot_id;
            let mut new_snapshot = prev_snapshot;
            new_snapshot.segments = vec![];
            new_snapshot.prev_snapshot_id = Some(prev_id);
            new_snapshot.summary = Default::default();
            let ctx: Arc<DatabendQueryContext> = io_ctx
                .get_user_data()?
                .expect("DatabendQueryContext should not be None");
            new_snapshot.snapshot_id = Uuid::new_v4();
            let new_snapshot_loc =
                util::snapshot_location(new_snapshot.snapshot_id.to_simple().to_string().as_str()); // TODO refine this
            let da = io_ctx.get_data_accessor()?;
            let bytes = serde_json::to_vec(&new_snapshot)?;
            da.put(&new_snapshot_loc, bytes).await?;

            let catalog = ctx.get_catalog();
            let table_id = self.get_id();
            // TODO backoff retry
            catalog
                .upsert_table_option(
                    table_id,
                    self.table_info.ident.version,
                    TBL_OPT_KEY_SNAPSHOT_LOC.to_string(),
                    new_snapshot_loc,
                )
                .await?
        }

        Ok(())
    }
}
