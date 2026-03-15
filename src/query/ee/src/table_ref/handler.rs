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

use chrono::Duration;
use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CommitTableBranchMetaReq;
use databend_common_meta_app::schema::CreateTableBranchReq;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::STAGED_BRANCH_MIN_LIFETIME;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_sql::binder::ConstraintExprBinder;
use databend_common_sql::plans::CreateTableBranchPlan;
use databend_common_sql::plans::CreateTableTagPlan;
use databend_common_sql::plans::DropTableBranchPlan;
use databend_common_sql::plans::DropTableTagPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::check_table_ref_access;
use databend_enterprise_table_ref_handler::TableRefHandler;
use databend_enterprise_table_ref_handler::TableRefHandlerWrapper;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::is_uuid_v7;
use databend_storages_common_table_meta::table::OPT_KEY_BASE_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_REFERENCED_BRANCH_IDS;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;

pub struct RealTableRefHandler {}

#[async_trait::async_trait]
impl TableRefHandler for RealTableRefHandler {
    #[async_backtrace::framed]
    async fn do_create_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableBranchPlan,
    ) -> Result<()> {
        check_table_ref_access(ctx.as_ref())?;

        let table = self
            .load_source_table(
                ctx.clone(),
                &plan.catalog,
                &plan.database,
                &plan.table,
                plan.branch.as_deref(),
            )
            .await?;
        let table_info = table.get_table_info();
        let source_table_id = table_info.ident.table_id;
        let seq = table_info.ident.seq;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let base_table_id = fuse_table.get_option(OPT_KEY_BASE_TABLE_ID, source_table_id);
        let db_id: u64 = table_info
            .meta
            .options
            .get(OPT_KEY_DATABASE_ID)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "missing {} in table options for table {}",
                    OPT_KEY_DATABASE_ID, source_table_id
                ))
            })?;
        let source_snapshot_location = match &plan.navigation {
            Some(navigation) => {
                fuse_table
                    .navigate_to_location(ctx.clone(), navigation)
                    .await?
            }
            None => fuse_table.snapshot_loc(),
        };

        let now = Utc::now();
        let mut branch_table_meta = TableMeta {
            created_on: now,
            updated_on: now,
            drop_on: None,
            statistics: TableStatistics::default(),
            ..table_info.meta.clone()
        };
        Self::remove_physical_location_options(&mut branch_table_meta.options);
        branch_table_meta
            .options
            .insert(OPT_KEY_BASE_TABLE_ID.to_string(), base_table_id.to_string());

        // Record which table IDs this new branch transitively references.
        // Not set when snapshot is empty (no segments to protect).
        // - Created from base table with data: "base_id"
        // - Created from branch A (refs "base_id"): "A_id,base_id"
        // - Created from branch B (refs "A_id,base_id"): "B_id,A_id,base_id"
        if source_snapshot_location.is_some() {
            let mut ref_ids = source_table_id.to_string();
            if let Some(inherited) = table_info.meta.options.get(OPT_KEY_REFERENCED_BRANCH_IDS) {
                if !inherited.is_empty() {
                    ref_ids.push(',');
                    ref_ids.push_str(inherited);
                }
            }
            branch_table_meta
                .options
                .insert(OPT_KEY_REFERENCED_BRANCH_IDS.to_string(), ref_ids);
        }

        let (new_snapshot, lvt_check) = if let Some(location) = source_snapshot_location {
            let Some(snapshot) = fuse_table
                .read_table_snapshot_with_location(Some(location.clone()))
                .await?
            else {
                return Err(ErrorCode::TableHistoricalDataNotFound(format!(
                    "Snapshot '{}' not found when creating BRANCH '{}'",
                    location, plan.branch_name
                )));
            };

            let Some(snapshot_timestamp) = snapshot.timestamp else {
                return Err(ErrorCode::IllegalReference(format!(
                    "Table {} snapshot lacks required timestamp",
                    source_table_id
                )));
            };
            if !is_uuid_v7(&snapshot.snapshot_id) {
                return Err(ErrorCode::IllegalReference(format!(
                    "Cannot create BRANCH '{}': snapshot '{}' is not based on uuid v7",
                    plan.branch_name, location
                )));
            }

            fuse_table.apply_navigation_metadata(&mut branch_table_meta, &snapshot)?;
            // Check constraints compatibility against the target snapshot schema.
            if !branch_table_meta.constraints.is_empty()
                && table_info.meta.schema.as_ref() != &snapshot.schema
            {
                let mut binder = ConstraintExprBinder::try_new(
                    ctx.clone(),
                    Arc::new((&snapshot.schema).into()),
                )?;
                branch_table_meta.constraints.retain(|name, constraint| {
                    binder.parse_and_bind(name, &constraint.expr()).is_ok()
                });
            }
            branch_table_meta.drop_on = Some(now);
            branch_table_meta.statistics = TableStatistics {
                number_of_rows: snapshot.summary.row_count,
                data_bytes: snapshot.summary.uncompressed_byte_size,
                compressed_data_bytes: snapshot.summary.compressed_byte_size,
                index_data_bytes: snapshot.summary.index_size,
                bloom_index_size: snapshot.summary.bloom_index_size,
                ngram_index_size: snapshot.summary.ngram_index_size,
                inverted_index_size: snapshot.summary.inverted_index_size,
                vector_index_size: snapshot.summary.vector_index_size,
                virtual_column_size: snapshot.summary.virtual_column_size,
                number_of_segments: Some(snapshot.segments.len() as u64),
                number_of_blocks: Some(snapshot.summary.block_count),
            };

            let mut branch_snapshot = TableSnapshot::try_from_previous(
                snapshot.clone(),
                None,
                ctx.get_table_meta_timestamps(fuse_table, Some(snapshot.clone()))?,
            )?;
            branch_snapshot.prev_snapshot_id = None;
            // Branch snapshots must not keep inherited base-table statistics files.
            branch_snapshot.table_statistics_location = None;
            if let Some(additional_stats_meta) =
                branch_snapshot.summary.additional_stats_meta.as_mut()
            {
                additional_stats_meta.location = ("".to_string(), 0);
            }
            branch_snapshot.cluster_key_meta = branch_table_meta.cluster_key_v2.clone();

            (
                Some(branch_snapshot),
                Some(TableLvtCheck {
                    tenant: ctx.get_tenant(),
                    time: snapshot_timestamp,
                }),
            )
        } else {
            (None, None)
        };

        let as_dropped = branch_table_meta.drop_on.is_some();
        let expire_at = plan.retain.map(|v| now + v);
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let branch_reply = catalog
            .create_table_branch(CreateTableBranchReq {
                name_ident: TableNameIdent::new(ctx.get_tenant(), &plan.database, &plan.table),
                db_id,
                table_id: base_table_id,
                source_table_id,
                branch_name: plan.branch_name.clone(),
                seq: MatchSeq::Exact(seq),
                table_meta: branch_table_meta,
                expire_at,
                as_dropped,
                lvt_check,
            })
            .await?;

        if !as_dropped {
            return Ok(());
        }

        let branch_table_info = Arc::new(TableInfo {
            ident: TableIdent {
                table_id: branch_reply.branch_id,
                seq: branch_reply.branch_id_seq,
            },
            desc: format!(
                "'{}'.'{}'/'{}'",
                plan.database, plan.table, plan.branch_name
            ),
            name: plan.table.clone(),
            meta: branch_reply.branch_meta.clone(),
            db_type: DatabaseType::NormalDB,
            catalog_info: Default::default(),
        });

        let branch_table = catalog.get_table_by_info(branch_table_info.as_ref())?;
        let branch_fuse_table = FuseTable::try_from_table(branch_table.as_ref())?;
        let new_snapshot = new_snapshot.ok_or_else(|| {
            ErrorCode::Internal("hidden branch creation must have a source snapshot")
        })?;
        let new_snapshot_location = branch_fuse_table
            .meta_location_generator()
            .gen_snapshot_location(&new_snapshot.snapshot_id, new_snapshot.format_version)?;
        branch_fuse_table
            .get_operator_ref()
            .write(&new_snapshot_location, new_snapshot.to_bytes()?)
            .await
            .map_err(ErrorCode::from)?;

        let committed_branch_meta = FuseTable::build_new_table_meta(
            &branch_table_info.meta,
            &new_snapshot_location,
            &new_snapshot,
        );

        let orphan_branch_name = branch_reply.orphan_branch_name.ok_or_else(|| {
            ErrorCode::Internal("orphan branch name must exist for hidden branch creation")
        })?;
        let retention_boundary = Utc::now()
            - std::cmp::max(
                Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64),
                STAGED_BRANCH_MIN_LIFETIME,
            );

        // If failed, the staged (hidden) branch and its snapshot file are left in place.
        // Vacuum GC will discover the orphan dropped-branch entry and clean up
        // all associated data (storage files, auto-increment keys, policies)
        // on its next run.
        catalog
            .commit_table_branch_meta(CommitTableBranchMetaReq {
                db_id,
                table_name: plan.table.clone(),
                table_id: base_table_id,
                branch_name: plan.branch_name.clone(),
                branch_id: branch_table_info.ident.table_id,
                seq: MatchSeq::Exact(branch_table_info.ident.seq),
                new_table_meta: committed_branch_meta,
                expire_at,
                orphan_branch_name: orphan_branch_name.clone(),
                retention_boundary,
            })
            .await
    }

    #[async_backtrace::framed]
    async fn do_create_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableTagPlan,
    ) -> Result<()> {
        check_table_ref_access(ctx.as_ref())?;

        let table = self
            .load_source_table(
                ctx.clone(),
                &plan.catalog,
                &plan.database,
                &plan.table,
                None,
            )
            .await?;
        let table_info = table.get_table_info();
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_loc = match &plan.navigation {
            Some(navigation) => {
                fuse_table
                    .navigate_to_location(ctx.clone(), navigation)
                    .await?
            }
            None => fuse_table.snapshot_loc(),
        }
        .ok_or_else(|| {
            ErrorCode::IllegalReference(format!(
                "The table '{}.{}' has no snapshot to create TAG '{}'",
                plan.database, plan.table, plan.name
            ))
        })?;

        let Some(snapshot) = fuse_table
            .read_table_snapshot_with_location(Some(snapshot_loc.clone()))
            .await?
        else {
            return Err(ErrorCode::TableHistoricalDataNotFound(format!(
                "Snapshot '{}' not found when creating TAG '{}'",
                snapshot_loc, plan.name
            )));
        };

        if !is_uuid_v7(&snapshot.snapshot_id) {
            return Err(ErrorCode::IllegalReference(format!(
                "Cannot create TAG '{}': snapshot '{}' is not based on uuid v7",
                plan.name, snapshot_loc
            )));
        }

        let Some(snapshot_timestamp) = snapshot.timestamp else {
            return Err(ErrorCode::IllegalReference(format!(
                "Table {} snapshot lacks required timestamp",
                table_info.ident.table_id
            )));
        };

        let catalog = ctx.get_catalog(&plan.catalog).await?;
        catalog
            .create_table_tag(CreateTableTagReq {
                table_id: table_info.ident.table_id,
                seq: MatchSeq::Exact(table_info.ident.seq),
                tag_name: plan.name.clone(),
                snapshot_loc,
                expire_at: plan.retain.map(|v| Utc::now() + v),
                lvt_check: TableLvtCheck {
                    tenant: ctx.get_tenant(),
                    time: snapshot_timestamp,
                },
            })
            .await
    }

    #[async_backtrace::framed]
    async fn do_drop_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableBranchPlan,
    ) -> Result<()> {
        check_table_ref_access(ctx.as_ref())?;

        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let branch_table = catalog
            .get_table_branch(
                &tenant,
                &plan.database,
                &plan.table,
                &plan.branch_name,
                true,
            )
            .await?;
        let branch_table_info = branch_table.get_table_info();
        let base_table_id = branch_table_info
            .options()
            .get(OPT_KEY_BASE_TABLE_ID)
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Failed to get '{}' for branch '{}'",
                    OPT_KEY_BASE_TABLE_ID, plan.branch_name
                ))
            })?;

        catalog
            .drop_table_branch(DropTableBranchReq {
                tenant,
                table_id: base_table_id,
                branch_name: plan.branch_name.clone(),
                branch_id: branch_table_info.ident.table_id,
            })
            .await
    }

    #[async_backtrace::framed]
    async fn do_drop_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableTagPlan,
    ) -> Result<()> {
        check_table_ref_access(ctx.as_ref())?;

        let table = self
            .load_source_table(
                ctx.clone(),
                &plan.catalog,
                &plan.database,
                &plan.table,
                None,
            )
            .await?;
        let table_id = table.get_table_info().ident.table_id;
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let seq_tag = catalog.get_table_tag(table_id, &plan.name, true).await?;
        let Some(seq_tag) = seq_tag else {
            return Err(ErrorCode::UnknownReference(format!(
                "Unknown tag '{}'",
                plan.name
            )));
        };

        catalog
            .drop_table_tag(DropTableTagReq {
                table_id,
                tag_name: plan.name.clone(),
                seq: MatchSeq::Exact(seq_tag.seq),
            })
            .await
    }
}

impl RealTableRefHandler {
    pub fn init() -> Result<()> {
        let handler = RealTableRefHandler {};
        let wrapper = TableRefHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn remove_physical_location_options(options: &mut std::collections::BTreeMap<String, String>) {
        for key in [
            OPT_KEY_STORAGE_PREFIX,
            OPT_KEY_TEMP_PREFIX,
            OPT_KEY_SNAPSHOT_LOCATION,
            OPT_KEY_LEGACY_SNAPSHOT_LOC,
            OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG,
            OPT_KEY_LOCATION,
            OPT_KEY_TABLE_ATTACHED_DATA_URI,
        ] {
            options.remove(key);
        }
    }

    #[async_backtrace::framed]
    async fn load_source_table(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: &str,
        database: &str,
        table_name: &str,
        branch: Option<&str>,
    ) -> Result<Arc<dyn Table>> {
        let table = ctx
            .get_table_with_branch(catalog, database, table_name, branch)
            .await?;

        if table.is_temp() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is temporary, can't create table refs",
                database, table_name
            )));
        }

        let table_info = table.get_table_info();
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' uses engine '{}', only FUSE tables support table refs",
                database,
                table_name,
                table_info.engine(),
            )));
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        if fuse_table.is_transient() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is transient, can't create table refs",
                database, table_name
            )));
        }

        Ok(table)
    }
}
