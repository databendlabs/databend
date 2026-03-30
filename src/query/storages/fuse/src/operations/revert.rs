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

use databend_common_catalog::table::NavigationDescriptor;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_sql::binder::validate_constraints_by_schema;
use databend_common_sql::binder::validate_table_indexes_compatible_with_schema;
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
        let table = self
            .navigate_for_revert(&ctx, &navigation_descriptor.point)
            .await?;
        let table_reverting_to = FuseTable::try_from_table(table.as_ref())?;
        let table_info = table_reverting_to.get_table_info();

        // shortcut. if reverting to the same point, just return ok
        if self.snapshot_loc() == table_reverting_to.snapshot_loc() {
            return Ok(());
        }

        // 2. prepare table meta which being reverted to
        let table_meta_to_be_committed = table_reverting_to.table_info.meta.clone();

        // 3. prepare the request
        //  using the CURRENT version as the base table version
        let base_version = self.table_info.ident.seq;
        let table_id = table_info.ident.table_id;
        let catalog = ctx.get_catalog(self.table_info.catalog()).await?;
        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(base_version),
            new_table_meta: table_meta_to_be_committed.clone(),
            base_snapshot_location: self.snapshot_loc(),
            lvt_check: None,
        };

        // 4. let's roll
        let reply = catalog.update_single_table_meta(req, table_info).await;
        if reply.is_ok() {
            // try keeping the snapshot hit
            let snapshot_location = table_reverting_to.snapshot_loc().ok_or_else(|| {
                    ErrorCode::Internal("internal error, fuse table which navigated to given point has no snapshot location")
                })?;

            // Left a hint file which indicates the location of the latest snapshot
            let snapshot_hint_writer =
                SnapshotHintWriter::new(ctx.as_ref(), &table_reverting_to.operator);
            snapshot_hint_writer
                .write_last_snapshot_hint(
                    &table_reverting_to.meta_location_generator,
                    &snapshot_location,
                    &table_meta_to_be_committed,
                )
                .await;
        };

        reply.map(|_| ())
    }

    #[async_backtrace::framed]
    async fn navigate_for_revert(
        &self,
        ctx: &Arc<dyn TableContext>,
        point: &NavigationPoint,
    ) -> Result<Arc<FuseTable>> {
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

        if self.apply_snapshot_metadata_to_meta(&mut table_info.meta, snapshot.as_ref())? {
            self.validate_revert_metadata(ctx, &table_info.meta).await?;
        }

        table_info.meta.options.insert(
            databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION.to_string(),
            snapshot_loc,
        );
        self.apply_snapshot_statistics(&mut table_info.meta, snapshot.as_ref());

        Ok(FuseTable::create_without_refresh_table_info(
            table_info,
            ctx.get_settings().get_s3_storage_class()?,
        )?
        .into())
    }

    async fn validate_revert_metadata(
        &self,
        ctx: &Arc<dyn TableContext>,
        target_meta: &TableMeta,
    ) -> Result<()> {
        let target_schema = target_meta.schema.as_ref();
        let column_ids = target_schema.to_column_ids();
        let catalog = ctx.get_catalog(self.table_info.catalog()).await?;
        let tenant = ctx.get_tenant();

        validate_table_indexes_compatible_with_schema(
            ctx.clone(),
            catalog.as_ref(),
            &tenant,
            self.get_id(),
            target_schema,
        )
        .await?;

        validate_constraints_by_schema(
            ctx.clone(),
            &self.table_info.meta.constraints,
            target_schema,
        )?;

        let incompatible_indexes = target_meta
            .indexes
            .values()
            .filter(|index| {
                !index
                    .column_ids
                    .iter()
                    .all(|column_id| column_ids.contains(column_id))
            })
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        if !incompatible_indexes.is_empty() {
            return Err(ErrorCode::IllegalReference(format!(
                "Cannot flashback: index(es) {:?} reference columns that do not exist \
                 in the target schema. Please DROP the index before proceeding.",
                incompatible_indexes
            )));
        }

        let broken_mask_column_ids = target_meta
            .column_mask_policy_columns_ids
            .iter()
            .filter_map(|(column_id, policy_map)| {
                let target_missing = !column_ids.contains(column_id);
                let referenced_missing = policy_map
                    .columns_ids
                    .iter()
                    .any(|id| !column_ids.contains(id));

                (target_missing || referenced_missing).then_some(*column_id)
            })
            .collect::<Vec<_>>();
        if !broken_mask_column_ids.is_empty() {
            return Err(ErrorCode::IllegalReference(format!(
                "Cannot navigate to target snapshot: masking policy on column ID(s) {:?} \
                 references columns that do not exist in the target schema. \
                 Please unset the masking policy before proceeding.",
                broken_mask_column_ids
            )));
        }

        if let Some(policy_map) = &target_meta.row_access_policy_columns_ids {
            let missing_column_ids = policy_map
                .columns_ids
                .iter()
                .filter(|id| !column_ids.contains(id))
                .copied()
                .collect::<Vec<_>>();
            if !missing_column_ids.is_empty() {
                return Err(ErrorCode::IllegalReference(format!(
                    "Cannot navigate to target snapshot: row access policy references \
                     column ID(s) {:?} that do not exist in the target schema. \
                     Please drop the row access policy before proceeding.",
                    missing_column_ids
                )));
            }
        }

        Ok(())
    }
}
