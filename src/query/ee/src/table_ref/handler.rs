// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::SnapshotRef;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::CreateTableRefPlan;
use databend_common_sql::plans::DropTableRefPlan;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_table_ref_handler::TableRefHandler;
use databend_enterprise_table_ref_handler::TableRefHandlerWrapper;
use databend_storages_common_table_meta::meta::TableSnapshot;

pub struct RealTableRefHandler {}

#[async_trait::async_trait]
impl TableRefHandler for RealTableRefHandler {
    #[async_backtrace::framed]
    async fn do_create_table_ref(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableRefPlan,
    ) -> Result<()> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(&plan.catalog).await?;

        let table = catalog
            .get_table(&tenant, &plan.database, &plan.table)
            .await?;
        let table_info = table.get_table_info();
        if table.is_temp() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is temporary, can't create {}",
                plan.database, plan.table, plan.ref_type
            )));
        }
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' uses engine '{}', only FUSE tables support {} creation",
                plan.database,
                plan.table,
                table_info.engine(),
                plan.ref_type
            )));
        }
        let refs = &table_info.meta.refs;
        if refs.contains_key(&plan.ref_name) {
            return Err(ErrorCode::ReferenceAlreadyExists(format!(
                "The table '{}.{}' already has a reference named '{}'",
                plan.database, plan.table, plan.ref_name
            )));
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        if fuse_table.is_transient() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is transient, can't create {}",
                plan.database, plan.table, plan.ref_type
            )));
        }
        let seq = table_info.ident.seq;
        let table_id = table_info.ident.table_id;
        let base_loc = fuse_table.snapshot_loc();
        let snapshot_loc = match &plan.navigation {
            Some(navigation) => {
                fuse_table
                    .navigate_to_location(ctx.clone(), navigation)
                    .await?
            }
            None => base_loc,
        };

        let (new_snapshot, prev_ts) = if let Some(snapshot) = fuse_table
            .read_table_snapshot_with_location(snapshot_loc)
            .await?
        {
            let mut new_snapshot = TableSnapshot::try_from_previous(
                snapshot.clone(),
                Some(seq),
                ctx.get_table_meta_timestamps(fuse_table, Some(snapshot.clone()))?,
            )?;
            new_snapshot.prev_snapshot_id = None;
            (new_snapshot, snapshot.timestamp)
        } else {
            let new_snapshot = TableSnapshot::try_new(
                Some(seq),
                None,
                table_info.schema().as_ref().clone(),
                Default::default(),
                vec![],
                None,
                ctx.get_table_meta_timestamps(fuse_table, None)?,
            )?;
            (new_snapshot, None)
        };

        // check least visible time
        let mut lvt_check = None;
        if plan
            .navigation
            .as_ref()
            .is_some_and(|v| !matches!(v, NavigationPoint::TableRef { .. }))
        {
            if let Some(ts) = prev_ts {
                let current_lvt = catalog
                    .get_table_lvt(&LeastVisibleTimeIdent::new(&tenant, table_id))
                    .await?;
                if let Some(lvt) = current_lvt {
                    if ts < lvt.time {
                        return Err(ErrorCode::IllegalReference(format!(
                            "Cannot create {} '{}', because the referred snapshot timestamp {} is older than table least visible time {}",
                            plan.ref_type, plan.ref_name, ts, lvt.time,
                        )));
                    }
                    lvt_check = Some(TableLvtCheck {
                        tenant: tenant.clone(),
                        lvt,
                    });
                }
            }
        }

        // write down new snapshot
        let new_snapshot_location = fuse_table
            .meta_location_generator()
            .ref_snapshot_location_from_uuid(
                seq,
                &new_snapshot.snapshot_id,
                new_snapshot.format_version,
            )?;
        let data = new_snapshot.to_bytes()?;
        fuse_table
            .get_operator_ref()
            .write(&new_snapshot_location, data)
            .await?;

        let expire_at = plan.retain.map(|v| Utc::now() + v);
        let mut new_table_meta = table_info.meta.clone();
        new_table_meta
            .refs
            .insert(plan.ref_name.clone(), SnapshotRef {
                id: seq,
                expire_at,
                typ: plan.ref_type.clone(),
                loc: new_snapshot_location,
            });
        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(seq),
            new_table_meta,
            base_snapshot_location: fuse_table.snapshot_loc(),
            lvt_check,
        };

        // If update fails, cleanup the ref directory
        if let Err(e) = catalog.update_single_table_meta(req, table_info).await {
            clearup_ref_dir(fuse_table, seq).await;
            return Err(e);
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn do_drop_table_ref(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableRefPlan,
    ) -> Result<()> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(&plan.catalog).await?;

        let table = catalog
            .get_table(&tenant, &plan.database, &plan.table)
            .await?;
        let table_info = table.get_table_info();
        let refs = &table_info.meta.refs;
        let Some(table_ref) = refs.get(&plan.ref_name) else {
            return Err(ErrorCode::UnknownReference(format!(
                "Unknown {} '{}' in table '{}.{}'",
                plan.ref_type, plan.ref_name, plan.database, plan.table
            )));
        };
        if table_ref.typ != plan.ref_type {
            return Err(ErrorCode::MismatchedReferenceType(format!(
                "'{}' is a {} reference, please use 'ALTER TABLE {}.{} DROP {} {}' instead.",
                plan.ref_name,
                table_ref.typ,
                plan.database,
                plan.table,
                table_ref.typ,
                plan.ref_name,
            )));
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let mut new_table_meta = table_info.meta.clone();
        new_table_meta.refs.remove(&plan.ref_name);
        let req = UpdateTableMetaReq {
            table_id: table_info.ident.table_id,
            seq: MatchSeq::Exact(table_info.ident.seq),
            new_table_meta,
            base_snapshot_location: fuse_table.snapshot_loc(),
            lvt_check: None,
        };
        catalog.update_single_table_meta(req, table_info).await?;

        // clear the ref snapshot.
        clearup_ref_dir(fuse_table, table_ref.id).await;
        Ok(())
    }
}

impl RealTableRefHandler {
    pub fn init() -> Result<()> {
        let handler = RealTableRefHandler {};
        let wrapper = TableRefHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}

async fn clearup_ref_dir(fuse_table: &FuseTable, table_ref_id: u64) {
    let ref_dir = format!(
        "{}{}/",
        fuse_table
            .meta_location_generator()
            .ref_snapshot_location_prefix(),
        table_ref_id
    );
    if let Err(cleanup_err) = fuse_table.get_operator_ref().remove_all(&ref_dir).await {
        log::warn!(
            "Failed to cleanup ref directory {}: {}",
            ref_dir,
            cleanup_err
        );
    }
}
