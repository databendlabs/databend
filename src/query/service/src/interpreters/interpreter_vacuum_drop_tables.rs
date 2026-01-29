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

use std::cmp::min;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::Duration;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::GarbageCollectionApi;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_sql::plans::VacuumDropTablePlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use log::info;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

const DRY_RUN_LIMIT: usize = 1000;

pub struct VacuumDropTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumDropTablePlan,
}

impl VacuumDropTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumDropTablePlan) -> Result<Self> {
        Ok(VacuumDropTablesInterpreter { ctx, plan })
    }

    /// Vacuum metadata of dropped tables and databases.
    ///
    /// Returns the approximate number of metadata keys removed.
    async fn gc_drop_tables(
        &self,
        catalog: Arc<dyn Catalog>,
        drop_ids: Vec<DroppedId>,
    ) -> Result<usize> {
        info!(
            "vacuum metadata of dropped table from db {:?}",
            self.plan.database,
        );

        let mut drop_db_ids = vec![];
        let mut drop_db_table_ids = vec![];
        for drop_id in drop_ids {
            match drop_id {
                DroppedId::Db { .. } => {
                    drop_db_ids.push(drop_id);
                }
                DroppedId::Table { .. } => {
                    drop_db_table_ids.push(drop_id);
                }
            }
        }

        info!(
            "found {} database meta data and {} table metadata need to be cleaned",
            drop_db_ids.len(),
            drop_db_table_ids.len()
        );

        let chunk_size = 50;

        let mut num_meta_keys_removed = 0;

        // Tag references cleanup is now handled in meta-service layer:
        // - Table tag refs: cleaned up in remove_data_for_dropped_table (garbage_collection_api.rs)
        // - Database tag refs: cleaned up in drop_database_meta (database_util.rs)

        // first gc drop table ids
        for c in drop_db_table_ids.chunks(chunk_size) {
            info!("vacuum drop {} table ids: {:?}", c.len(), c);
            let req = GcDroppedTableReq {
                tenant: self.ctx.get_tenant(),
                catalog: self.plan.catalog.clone(),
                drop_ids: c.to_vec(),
            };
            num_meta_keys_removed += catalog.gc_drop_tables(req).await?;
        }

        // then gc drop db ids
        for c in drop_db_ids.chunks(chunk_size) {
            info!("vacuum drop {} db ids: {:?}", c.len(), c);
            let req = GcDroppedTableReq {
                tenant: self.ctx.get_tenant(),
                catalog: self.plan.catalog.clone(),
                drop_ids: c.to_vec(),
            };
            num_meta_keys_removed += catalog.gc_drop_tables(req).await?;
        }

        Ok(num_meta_keys_removed)
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumDropTablesInterpreter {
    fn name(&self) -> &str {
        "VacuumDropTablesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

        let ctx = self.ctx.clone();
        let duration = Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);

        let retention_time = chrono::Utc::now() - duration;

        // Set vacuum timestamp before starting the vacuum operation (only in non-dry-run mode)
        // This ensures undrop operations after this point will be blocked
        if self.plan.option.dry_run.is_none() {
            let tenant = ctx.get_tenant();
            let meta_api = UserApiProvider::instance().get_meta_store_client();

            // CRITICAL: Must succeed in setting vacuum timestamp before proceeding
            // If this fails, vacuum operation should not proceed to prevent data loss
            meta_api
                .fetch_set_vacuum_timestamp(&tenant, retention_time)
                .await
                .map_err(|e| {
                    ErrorCode::MetaStorageError(format!(
                        "Failed to set vacuum timestamp before vacuum operation: {}. Vacuum aborted to prevent data inconsistency.",
                        e
                    ))
                })?;
        }
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        info!(
            "=== VACUUM DROP TABLE STARTED === db: {:?}, retention_days: {}, retention_time: {:?}",
            self.plan.database,
            ctx.get_settings().get_data_retention_time_in_days()?,
            retention_time
        );
        // if database if empty, vacuum all tables
        let database_name = if self.plan.database.is_empty() {
            None
        } else {
            Some(self.plan.database.clone())
        };

        let tenant = self.ctx.get_tenant();
        let (tables, drop_ids) = catalog
            .get_drop_table_infos(ListDroppedTableReq::new4(
                &tenant,
                database_name,
                Some(retention_time),
                self.plan.option.limit,
            ))
            .await?;

        // map: table id to its belonging db id
        let mut containing_db = BTreeMap::new();
        for drop_id in drop_ids.iter() {
            if let DroppedId::Table { name, id } = drop_id {
                containing_db.insert(id.table_id, name.db_id);
            }
        }

        info!(
            "vacuum drop table from db {:?}, found {} tables: [{}], drop_ids: {:?}",
            self.plan.database,
            tables.len(),
            tables
                .iter()
                .map(|t| format!(
                    "{}(id:{})",
                    t.get_table_info().name,
                    t.get_table_info().ident.table_id
                ))
                .collect::<Vec<_>>()
                .join(", "),
            drop_ids
        );

        // Filter out read-only tables and views.
        // Note: The drop_ids list still includes view IDs
        let (views, tables): (Vec<_>, Vec<_>) = tables
            .into_iter()
            .filter(|tbl| !tbl.as_ref().is_read_only())
            .partition(|tbl| tbl.get_table_info().meta.engine == VIEW_ENGINE);

        {
            let view_ids = views
                .into_iter()
                .map(|v| v.get_table_id())
                .collect::<Vec<_>>();
            info!("view ids excluded from purging data: {:?}", view_ids);
        }

        info!(
            "after filter read-only tables: {} tables remain: [{}]",
            tables.len(),
            tables
                .iter()
                .map(|t| format!(
                    "{}(id:{})",
                    t.get_table_info().name,
                    t.get_table_info().ident.table_id
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let tables_count = tables.len();

        let handler = get_vacuum_handler();
        let threads_nums = self.ctx.get_settings().get_max_vacuum_threads()? as usize;
        let (files_opt, failed_tables) = handler
            .do_vacuum_drop_tables(
                threads_nums,
                tables,
                if self.plan.option.dry_run.is_some() {
                    Some(DRY_RUN_LIMIT)
                } else {
                    None
                },
            )
            .await?;

        let failed_db_ids = failed_tables
            .iter()
            // Safe unwrap: the map is built from drop_ids
            .map(|id| *containing_db.get(id).unwrap())
            .collect::<HashSet<_>>();

        let mut num_meta_keys_removed = 0;
        // gc metadata only when not dry run
        if self.plan.option.dry_run.is_none() {
            let mut success_dropped_ids = vec![];
            // Since drop_ids contains view IDs, any views (if present) will be added to
            // the success_dropped_id list, with removal from the meta-server attempted later.
            for drop_id in drop_ids {
                match &drop_id {
                    DroppedId::Db { db_id, db_name: _ } => {
                        if !failed_db_ids.contains(db_id) {
                            success_dropped_ids.push(drop_id);
                        }
                    }
                    DroppedId::Table { name: _, id } => {
                        if !failed_tables.contains(&id.table_id) {
                            success_dropped_ids.push(drop_id);
                        }
                    }
                }
            }
            info!(
                "vacuum drop table summary - failed dbs: {}, failed tables: {}, successfully cleaned: {} items",
                failed_db_ids.len(),
                failed_tables.len(),
                success_dropped_ids.len()
            );
            if !failed_tables.is_empty() {
                info!("failed table ids: {:?}", failed_tables);
            }

            num_meta_keys_removed = self.gc_drop_tables(catalog, success_dropped_ids).await?;
        }

        let success_count = tables_count as u64 - failed_tables.len() as u64;
        let failed_count = failed_tables.len() as u64;

        info!(
            "=== VACUUM DROP TABLE COMPLETED === success: {}, failed: {}, total: {}, num_meta_keys_removed: {}",
            success_count, failed_count, tables_count, num_meta_keys_removed
        );

        match files_opt {
            None => PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                UInt64Type::from_data(vec![success_count]),
                UInt64Type::from_data(vec![failed_count]),
            ])]),
            Some(purge_files) => {
                let mut len = min(purge_files.len(), DRY_RUN_LIMIT);
                if let Some(limit) = self.plan.option.limit {
                    len = min(len, limit);
                }
                let purge_files = &purge_files[0..len];
                let mut table_file_sizes = HashMap::new();
                for (table_name, file, file_size) in purge_files {
                    table_file_sizes
                        .entry(table_name)
                        .and_modify(|file_sizes: &mut Vec<(String, u64)>| {
                            file_sizes.push((file.to_string(), *file_size))
                        })
                        .or_insert(vec![(file.to_string(), *file_size)]);
                }

                if let Some(summary) = self.plan.option.dry_run {
                    if summary {
                        let mut tables = vec![];
                        let mut total_files = vec![];
                        let mut total_size = vec![];
                        for (table, file_sizes) in table_file_sizes {
                            tables.push(table.to_string());
                            total_files.push(file_sizes.len() as u64);
                            total_size.push(file_sizes.into_iter().map(|(_, num)| num).sum());
                        }

                        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                            StringType::from_data(tables),
                            UInt64Type::from_data(total_files),
                            UInt64Type::from_data(total_size),
                        ])])
                    } else {
                        let mut tables = Vec::with_capacity(len);
                        let mut files = Vec::with_capacity(len);
                        let mut file_size = Vec::with_capacity(len);
                        for (table, file_sizes) in table_file_sizes {
                            for (file, size) in file_sizes {
                                tables.push(table.to_string());
                                files.push(file);
                                file_size.push(size);
                            }
                        }

                        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                            StringType::from_data(tables),
                            StringType::from_data(files),
                            UInt64Type::from_data(file_size),
                        ])])
                    }
                } else {
                    unreachable!();
                }
            }
        }
    }
}
