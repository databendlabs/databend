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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UpsertTableOptionReq;
use common_planners::TruncateTablePlan;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::FUSE_OPT_KEY_SNAPSHOT_LOC;

impl FuseTable {
    #[inline]
    pub async fn do_truncate(&self, ctx: Arc<QueryContext>, plan: TruncateTablePlan) -> Result<()> {
        if let Some(prev_snapshot) = self.read_table_snapshot(ctx.as_ref()).await? {
            let prev_id = prev_snapshot.snapshot_id;

            let new_snapshot = TableSnapshot {
                format_version: 1,
                snapshot_id: Uuid::new_v4(),
                prev_snapshot_id: Some((prev_id, prev_snapshot.format_version)),
                schema: prev_snapshot.schema.clone(),
                summary: Default::default(),
                segments: vec![],
            };
            let loc = self.meta_location_generator();
            let new_snapshot_loc = loc.snapshot_location_from_uuid(&new_snapshot.snapshot_id);
            let operator = ctx.get_storage_operator()?;
            let bytes = serde_json::to_vec(&new_snapshot)?;
            operator
                .object(&new_snapshot_loc)
                .writer()
                .write_bytes(bytes)
                .await
                .map_err(|e| ErrorCode::DalTransportError(e.to_string()))?;

            if plan.purge {
                let keep_last_snapshot = false;
                self.do_optimize(ctx.clone(), keep_last_snapshot).await?
            }
            ctx.get_catalog()
                .upsert_table_option(UpsertTableOptionReq::new(
                    &self.table_info.ident,
                    FUSE_OPT_KEY_SNAPSHOT_LOC,
                    new_snapshot_loc,
                ))
                .await?;
        }

        Ok(())
    }
}
