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

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::table::NavigationDescriptor;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_meta_client::types::MatchSeq;

use crate::FuseTable;
use crate::io::SnapshotsIO;
use crate::operations::SnapshotHintWriter;

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        navigation_descriptor: NavigationDescriptor,
    ) -> Result<()> {
        // 1. try navigate to the point
        let (table_reverting_to, snapshot_timestamp) = self
            .navigate_for_revert(&ctx, &navigation_descriptor.point)
            .await?;

        let target_snapshot_location = table_reverting_to.snapshot_loc().ok_or_else(|| {
            ErrorCode::Internal(
                "internal error, fuse table which navigated to given point has no snapshot location",
            )
        })?;
        let current_snapshot_location = self.snapshot_loc();
        // shortcut. if reverting to the same point, just return ok
        if current_snapshot_location.as_deref() == Some(&target_snapshot_location) {
            return Ok(());
        }

        let snapshot_timestamp = snapshot_timestamp
            .ok_or_else(|| ErrorCode::Internal("FLASHBACK target snapshot has no timestamp"))?;
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(self.table_info.catalog()).await?;
        let req = UpdateTableMetaReq {
            table_id: self.table_info.ident.table_id,
            seq: MatchSeq::Exact(self.table_info.ident.seq),
            new_table_meta: table_reverting_to.table_info.meta.clone(),
            base_snapshot_location: current_snapshot_location,
            lvt_check: Some(TableLvtCheck {
                time: snapshot_timestamp,
                touch: true,
            }),
        };

        // Publish the historical metadata.
        catalog
            .update_single_table_meta(&tenant, req, &self.table_info)
            .await?;

        // Leave a hint file indicating the latest snapshot location.
        let snapshot_hint_writer =
            SnapshotHintWriter::new(ctx.as_ref(), &table_reverting_to.operator);
        snapshot_hint_writer
            .write_last_snapshot_hint(
                &table_reverting_to.meta_location_generator,
                &target_snapshot_location,
                &table_reverting_to.table_info.meta,
            )
            .await;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn navigate_for_revert(
        &self,
        ctx: &Arc<dyn TableContext>,
        point: &NavigationPoint,
    ) -> Result<(Box<FuseTable>, Option<DateTime<Utc>>)> {
        let Some(snapshot_loc) = self.navigate_to_location(ctx.clone(), point).await? else {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        };
        let (snapshot, format_version) =
            SnapshotsIO::read_snapshot(snapshot_loc, self.get_operator(), true).await?;

        let mut table_info = self.table_info.clone();
        let snapshot_loc = self
            .meta_location_generator
            .gen_snapshot_location(&snapshot.snapshot_id, format_version)?;

        self.apply_snapshot_metadata_to_meta(&mut table_info.meta, snapshot.as_ref())?;
        FuseTable::prepare_persistent_navigation_metadata(&mut table_info.meta)?;
        self.validate_persistent_navigation_metadata(ctx.clone(), &table_info.meta, "flashback")
            .await?;

        table_info.meta.options.insert(
            databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION.to_string(),
            snapshot_loc,
        );
        Self::apply_snapshot_statistics(&mut table_info.meta, snapshot.as_ref());

        Ok((
            FuseTable::create_without_refresh_table_info(
                table_info,
                ctx.get_settings().get_s3_storage_class()?,
            )?,
            snapshot.timestamp,
        ))
    }
}
