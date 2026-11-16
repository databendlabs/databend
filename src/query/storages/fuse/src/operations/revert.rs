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

use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::table::NavigationDescriptor;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_meta_app::schema::OPT_KEY_CLONE_GROUP_ID;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_sql::binder::validate_constraints_by_schema;
use databend_common_sql::validate_stored_ttl_expr;
use databend_meta_client::types::MatchSeq;

use crate::FuseTable;
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

        // Legacy snapshots may have no timestamp. Reject them with a user-facing error here;
        // a missing timestamp on a newly generated commit remains an internal invariant error.
        if snapshot_timestamp.is_none()
            && self
                .table_info
                .meta
                .options
                .contains_key(OPT_KEY_CLONE_GROUP_ID)
        {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "Cannot FLASHBACK a clone-group member to a snapshot with no timestamp: \
                 its GC safety cannot be verified",
            ));
        }

        // Only clone-group members check and touch the fence; ordinary FLASHBACK is unchanged.
        // A live TAG preserves reads but does not permit restoring clone-group history below LVT.
        let lvt_check =
            Self::build_table_lvt_check(&self.table_info, snapshot_timestamp)?.map(|mut check| {
                check.touch = true;
                check
            });
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(self.table_info.catalog()).await?;
        let req = UpdateTableMetaReq {
            table_id: self.table_info.ident.table_id,
            seq: MatchSeq::Exact(self.table_info.ident.seq),
            new_table_meta: table_reverting_to.table_info.meta.clone(),
            base_snapshot_location: current_snapshot_location,
            lvt_check,
        };

        // Publish the historical metadata.
        catalog
            .update_single_table_meta(&tenant, req, &self.table_info)
            .await
            .map_err(|err| {
                if let NavigationPoint::TableTag(name) = &navigation_descriptor.point
                    && err.code() == ErrorCode::TABLE_SNAPSHOT_EXPIRED
                {
                    return err.add_message(format!(
                        "Cannot FLASHBACK to TAG '{name}': clone group members cannot restore \
                         snapshots below LVT. An active TAG preserves historical reads."
                    ));
                }
                err
            })?;

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
            Self::read_navigation_snapshot(snapshot_loc, self.get_operator(), "revert point")
                .await?;

        let mut table_info = self.table_info.clone();
        let snapshot_loc = self
            .meta_location_generator
            .gen_snapshot_location(&snapshot.snapshot_id, format_version)?;

        if self.apply_snapshot_metadata_to_meta(&mut table_info.meta, snapshot.as_ref())? {
            // Restore the original FLASHBACK behavior for non-versioned derived and legacy metadata.
            table_info.meta.virtual_schema = None;
            table_info.meta.column_mask_policy = None;
            table_info.meta.row_access_policy = None;
            self.validate_revert_metadata(ctx, &table_info.meta).await?;
        }

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

    async fn validate_revert_metadata(
        &self,
        ctx: &Arc<dyn TableContext>,
        target_meta: &TableMeta,
    ) -> Result<()> {
        let target_schema = target_meta.schema.as_ref();
        let target_column_ids: HashSet<ColumnId> =
            target_schema.to_column_ids().into_iter().collect();

        if let Some(ttl) = &target_meta.ttl {
            validate_stored_ttl_expr(ctx.clone(), target_meta.schema.clone(), ttl).map_err(
                |e| {
                    ErrorCode::IllegalReference(format!(
                        "Cannot flashback: TTL '{ttl}' is invalid for the target schema: {}. \
                     Please REMOVE TTL before proceeding.",
                        e.message()
                    ))
                },
            )?;
        }

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
                    .all(|column_id| target_column_ids.contains(column_id))
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
                let target_missing = !target_column_ids.contains(column_id);
                let referenced_missing = policy_map
                    .columns_ids
                    .iter()
                    .any(|id| !target_column_ids.contains(id));

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
                .filter(|id| !target_column_ids.contains(id))
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
