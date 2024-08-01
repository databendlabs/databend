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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::field_default_value;
use databend_common_sql::plans::AddColumnOption;
use databend_common_sql::plans::AddTableColumnPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_share::update_share_table_info;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use log::debug;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::interpreter_table_create::is_valid_column;
use crate::interpreters::interpreter_update::build_update_physical_plan;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::physical_plans::MutationKind;
use crate::sql::parse_computed_expr;

pub struct AddTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: AddTableColumnPlan,
}

impl AddTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AddTableColumnPlan) -> Result<Self> {
        Ok(AddTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AddTableColumnInterpreter {
    fn name(&self) -> &str {
        "AddTableColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        self.plan.is_deterministic
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        // check mutability
        tbl.check_mutable()?;

        let table_info = tbl.get_table_info();
        let engine = table_info.engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                &self.plan.database, &self.plan.table, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let mut new_table_meta = table_info.meta.clone();
        let field = self.plan.field.clone();
        if field.computed_expr().is_some() {
            let license_manager = get_license_manager();
            license_manager
                .manager
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        if field.default_expr().is_some() {
            let _ = field_default_value(self.ctx.clone(), &field)?;
        }
        is_valid_column(field.name())?;
        let index = match &self.plan.option {
            AddColumnOption::First => 0,
            AddColumnOption::After(name) => new_table_meta.schema.index_of(name)? + 1,
            AddColumnOption::End => new_table_meta.schema.num_fields(),
        };
        new_table_meta.add_column(&field, &self.plan.comment, index)?;

        let _ = generate_new_snapshot(tbl.as_ref(), &mut new_table_meta).await?;
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
        };

        let resp = catalog.update_single_table_meta(req, table_info).await?;

        if let Some(share_vec_table_infos) = &resp.share_vec_table_infos {
            for (share_name_vec, db_id, share_table_info) in share_vec_table_infos {
                update_share_table_info(
                    self.ctx.get_tenant().tenant_name(),
                    self.ctx.get_application_level_data_operator()?.operator(),
                    share_name_vec,
                    *db_id,
                    share_table_info,
                )
                .await?;
            }
        }

        // if the column is not deterministic, update to refresh the value with default expr
        if !self.plan.is_deterministic {
            debug!("ctx.id" = self.ctx.get_id().as_str(); "table_add_column_interpreter_execute");

            if check_deduplicate_label(self.ctx.clone()).await? {
                return Ok(PipelineBuildResult::create());
            }

            // clear tables cache and get table again.
            self.ctx.clear_tables_cache();
            let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

            if let Some(fuse_table) = tbl.as_any().downcast_ref::<FuseTable>() {
                let col_indices = vec![];
                let mut filters = None;
                let query_row_id_col = false;
                if let Some(snapshot) = fuse_table
                    .fast_update(
                        self.ctx.clone(),
                        &mut filters,
                        col_indices.clone(),
                        query_row_id_col,
                    )
                    .await?
                {
                    let partitions = fuse_table
                        .mutation_read_partitions(
                            self.ctx.clone(),
                            snapshot.clone(),
                            col_indices.clone(),
                            filters.clone(),
                            false,
                            false,
                        )
                        .await?;

                    let field_sql = field.default_expr().unwrap();
                    let expr = parse_computed_expr(
                        self.ctx.clone(),
                        DataSchemaRefExt::create(vec![]),
                        field_sql,
                    )?;
                    let expr: Expr<String> = expr.project_column_ref(|_| unreachable!());
                    let update_list = vec![(index, expr.as_remote_expr())];
                    // TODO
                    let computed_list = BTreeMap::new();

                    let is_distributed = !self.ctx.get_cluster().is_empty();
                    let physical_plan = build_update_physical_plan(
                        filters,
                        update_list,
                        computed_list,
                        partitions,
                        fuse_table.get_table_info().clone(),
                        col_indices,
                        snapshot,
                        query_row_id_col,
                        is_distributed,
                        self.ctx.clone(),
                    )?;

                    let mut build_res =
                        build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan)
                            .await?;
                    {
                        let hook_operator = HookOperator::create(
                            self.ctx.clone(),
                            catalog_name.to_string(),
                            db_name.to_string(),
                            tbl_name.to_string(),
                            MutationKind::Update,
                            // table lock has been added, no need to check.
                            LockTableOption::NoLock,
                        );
                        hook_operator
                            .execute_refresh(&mut build_res.main_pipeline)
                            .await;
                    }

                    // Add table lock.
                    let lock_guard = self
                        .ctx
                        .clone()
                        .acquire_table_lock(
                            catalog_name,
                            db_name,
                            tbl_name,
                            &LockTableOption::LockWithRetry,
                        )
                        .await?;

                    build_res.main_pipeline.add_lock_guard(lock_guard.clone());
                    return Ok(build_res);
                }
            }
        }
        Ok(PipelineBuildResult::create())
    }
}

pub(crate) async fn generate_new_snapshot(
    table: &dyn Table,
    new_table_meta: &mut TableMeta,
) -> Result<()> {
    if let Ok(fuse_table) = FuseTable::try_from_table(table) {
        if let Some(snapshot) = fuse_table.read_table_snapshot().await? {
            let mut new_snapshot = TableSnapshot::from_previous(
                snapshot.as_ref(),
                Some(fuse_table.get_table_info().ident.seq),
            );

            // replace schema
            new_snapshot.schema = new_table_meta.schema.as_ref().clone();

            // write down new snapshot
            let new_snapshot_location = fuse_table
                .meta_location_generator()
                .snapshot_location_from_uuid(&new_snapshot.snapshot_id, TableSnapshot::VERSION)?;

            let data = new_snapshot.to_bytes()?;
            fuse_table
                .get_operator_ref()
                .write(&new_snapshot_location, data)
                .await?;

            // write down hint
            FuseTable::write_last_snapshot_hint(
                fuse_table.get_operator_ref(),
                fuse_table.meta_location_generator(),
                new_snapshot_location.clone(),
            )
            .await;

            new_table_meta.options.insert(
                OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
                new_snapshot_location.clone(),
            );
        } else {
            info!("Snapshot not found, no need to generate new snapshot");
        }
    } else {
        info!("Not a fuse table, no need to generate new snapshot");
    }
    Ok(())
}
