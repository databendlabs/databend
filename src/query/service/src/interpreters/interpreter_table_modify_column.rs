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

use common_ast::ast::Identifier;
use common_ast::ast::ModifyColumnAction;
use common_ast::ast::TypeName;
use common_base::runtime::GlobalIORuntime;
use common_catalog::catalog::Catalog;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ComputedExpr;
use common_expression::DataSchema;
use common_expression::TableSchema;
use common_license::license::Feature::ComputedColumn;
use common_license::license::Feature::DataMask;
use common_license::license_manager::get_license_manager;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::SetTableColumnMaskPolicyAction;
use common_meta_app::schema::SetTableColumnMaskPolicyReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_sql::executor::DistributedInsertSelect;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::ModifyTableColumnPlan;
use common_sql::plans::Plan;
use common_sql::resolve_type_name;
use common_sql::BloomIndexColumns;
use common_sql::Planner;
use common_storages_fuse::FuseTable;
use common_storages_share::save_share_table_info;
use common_storages_view::view_table::VIEW_ENGINE;
use common_users::UserApiProvider;
use data_mask_feature::get_datamask_handler;
use storages_common_index::BloomIndex;
use storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use table_lock::TableLockHandlerWrapper;

use super::common::check_referenced_computed_columns;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ModifyTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: ModifyTableColumnPlan,
}

impl ModifyTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ModifyTableColumnPlan) -> Result<Self> {
        Ok(ModifyTableColumnInterpreter { ctx, plan })
    }

    // Set data mask policy to a column is a ee feature.
    async fn do_set_data_mask_policy(
        &self,
        catalog: Arc<dyn Catalog>,
        table: &Arc<dyn Table>,
        column: String,
        mask_name: String,
    ) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            DataMask,
        )?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();
        let policy = handler
            .get_data_mask(meta_api, self.ctx.get_tenant(), mask_name.clone())
            .await?;

        let schema = table.schema();
        let table_info = table.get_table_info();
        if let Some((_, data_field)) = schema.column_with_name(&column) {
            let data_type = data_field.data_type().to_string().to_lowercase();
            let policy_data_type = policy.args[0].1.to_string().to_lowercase();
            if data_type != policy_data_type {
                return Err(ErrorCode::UnmatchColumnDataType(format!(
                    "Column '{}' data type {} does not match to the mask policy type {}",
                    column, data_type, policy_data_type,
                )));
            }
        } else {
            return Err(ErrorCode::UnknownColumn(format!(
                "Cannot find column {}",
                column
            )));
        }

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let prev_column_mask_name =
            if let Some(column_mask_policy) = &table_info.meta.column_mask_policy {
                column_mask_policy.get(&column).cloned()
            } else {
                None
            };
        let req = SetTableColumnMaskPolicyReq {
            tenant: self.ctx.get_tenant(),
            seq: MatchSeq::Exact(table_version),
            table_id,
            column,
            action: SetTableColumnMaskPolicyAction::Set(mask_name, prev_column_mask_name),
        };

        let res = catalog.set_table_column_mask_policy(req).await?;

        if let Some(share_table_info) = res.share_table_info {
            save_share_table_info(
                &self.ctx.get_tenant(),
                self.ctx.get_data_operator()?.operator(),
                share_table_info,
            )
            .await?;
        }
        Ok(PipelineBuildResult::create())
    }

    // unset data mask policy to a column is a ee feature.
    async fn do_unset_data_mask_policy(
        &self,
        catalog: Arc<dyn Catalog>,
        table: &Arc<dyn Table>,
        column: String,
    ) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            DataMask,
        )?;

        let table_info = table.get_table_info();
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let prev_column_mask_name =
            if let Some(column_mask_policy) = &table_info.meta.column_mask_policy {
                column_mask_policy.get(&column).cloned()
            } else {
                None
            };

        if let Some(prev_column_mask_name) = prev_column_mask_name {
            let req = SetTableColumnMaskPolicyReq {
                tenant: self.ctx.get_tenant(),
                seq: MatchSeq::Exact(table_version),
                table_id,
                column,
                action: SetTableColumnMaskPolicyAction::Unset(prev_column_mask_name),
            };

            let res = catalog.set_table_column_mask_policy(req).await?;

            if let Some(share_table_info) = res.share_table_info {
                save_share_table_info(
                    &self.ctx.get_tenant(),
                    self.ctx.get_data_operator()?.operator(),
                    share_table_info,
                )
                .await?;
            }
        }

        Ok(PipelineBuildResult::create())
    }

    // Set data column type.
    async fn do_set_data_type(
        &self,
        table: &Arc<dyn Table>,
        column_name_types: &Vec<(Identifier, TypeName)>,
    ) -> Result<PipelineBuildResult> {
        let schema = table.schema().as_ref().clone();
        let table_info = table.get_table_info();
        let mut new_schema = schema.clone();

        // Add table lock heartbeat.
        let handler = TableLockHandlerWrapper::instance(self.ctx.clone());
        let mut heartbeat = handler
            .try_lock(self.ctx.clone(), table_info.clone())
            .await?;

        let catalog = self.ctx.get_catalog(table_info.catalog()).await?;
        let catalog_info = catalog.info();

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let prev_snapshot_id = fuse_table
            .read_table_snapshot()
            .await
            .map_or(None, |v| v.map(|snapshot| snapshot.snapshot_id));

        let mut bloom_index_cols = vec![];
        if let Some(v) = table_info.options().get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
            if let BloomIndexColumns::Specify(cols) = v.parse::<BloomIndexColumns>()? {
                bloom_index_cols = cols;
            }
        }

        for (column, type_name) in column_name_types {
            let column = column.to_string();
            if let Ok(i) = schema.index_of(&column) {
                let new_type = resolve_type_name(type_name)?;

                if new_type != new_schema.fields[i].data_type {
                    // Check if this column is referenced by computed columns.
                    let mut data_schema: DataSchema = table_info.schema().into();
                    data_schema.set_field_type(i, (&new_type).into());
                    check_referenced_computed_columns(
                        self.ctx.clone(),
                        Arc::new(data_schema),
                        &column,
                    )?;

                    // If the column is defined in bloom index columns,
                    // check whether the data type is supported for bloom index.
                    if bloom_index_cols.iter().any(|v| v.as_str() == column)
                        && !BloomIndex::supported_type(&new_type)
                    {
                        return Err(ErrorCode::TableOptionInvalid(format!(
                            "Unsupported data type '{}' for bloom index",
                            new_type
                        )));
                    }
                    new_schema.fields[i].data_type = new_type;
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    column
                )));
            }
        }
        // check if schema has changed
        if schema == new_schema {
            return Ok(PipelineBuildResult::create());
        }

        // 1. construct sql for selecting data from old table
        let mut sql = "select".to_string();
        schema
            .fields()
            .iter()
            .enumerate()
            .for_each(|(index, field)| {
                if index != schema.fields().len() - 1 {
                    sql = format!("{} `{}`,", sql, field.name.clone());
                } else {
                    sql = format!(
                        "{} `{}` from `{}`.`{}`",
                        sql,
                        field.name.clone(),
                        self.plan.database,
                        self.plan.table
                    );
                }
            });

        // 2. build plan by sql
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _extras) = planner.plan_sql(&sql).await?;

        // 3. build physical plan by plan
        let (select_plan, select_column_bindings) = match plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let mut builder1 =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                (builder1.build(&s_expr).await?, bind_context.columns.clone())
            }
            _ => unreachable!(),
        };

        // 4. define select schema and insert schema of DistributedInsertSelect plan
        let mut table_info = table.get_table_info().clone();
        table_info.meta.schema = new_schema.clone().into();
        let new_table = FuseTable::try_create(table_info)?;

        // 5. build DistributedInsertSelect plan
        let insert_plan =
            PhysicalPlan::DistributedInsertSelect(Box::new(DistributedInsertSelect {
                plan_id: select_plan.get_id(),
                input: Box::new(select_plan),
                catalog_info,
                table_info: new_table.get_table_info().clone(),
                select_schema: Arc::new(Arc::new(schema).into()),
                select_column_bindings,
                insert_schema: Arc::new(Arc::new(new_schema).into()),
                cast_needed: true,
            }));
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &insert_plan, false).await?;

        // 6. commit new meta schema and snapshots
        new_table.commit_insertion(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            None,
            true,
            prev_snapshot_id,
        )?;

        if build_res.main_pipeline.is_empty() {
            heartbeat.shutdown().await?;
        } else {
            build_res.main_pipeline.set_on_finished(move |may_error| {
                // shutdown table lock heartbeat.
                GlobalIORuntime::instance().block_on(async move { heartbeat.shutdown().await })?;
                match may_error {
                    None => Ok(()),
                    Some(error_code) => Err(error_code.clone()),
                }
            });
        }

        Ok(build_res)
    }

    async fn do_convert_stored_computed_column(
        &self,
        catalog: Arc<dyn Catalog>,
        table: &Arc<dyn Table>,
        table_meta: TableMeta,
        column: String,
    ) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            ComputedColumn,
        )?;

        let table_info = table.get_table_info();
        let schema = table.schema();
        let new_schema = if let Some((i, field)) = schema.column_with_name(&column) {
            match field.computed_expr {
                Some(ComputedExpr::Stored(_)) => {}
                _ => {
                    return Err(ErrorCode::UnknownColumn(format!(
                        "Column '{}' is not a stored computed column",
                        column
                    )));
                }
            }
            let mut new_field = field.clone();
            new_field.computed_expr = None;
            let mut fields = schema.fields().clone();
            fields[i] = new_field;
            TableSchema::new_from(fields, schema.metadata.clone())
        } else {
            return Err(ErrorCode::UnknownColumn(format!(
                "Cannot find column {}",
                column
            )));
        };

        let mut new_table_meta = table_meta;
        new_table_meta.schema = new_schema.into();

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            copied_files: None,
            deduplicated_label: None,
        };

        let res = catalog.update_table_meta(table_info, req).await?;

        if let Some(share_table_info) = res.share_table_info {
            save_share_table_info(
                &self.ctx.get_tenant(),
                self.ctx.get_data_operator()?.operator(),
                share_table_info,
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for ModifyTableColumnInterpreter {
    fn name(&self) -> &str {
        "ModifyTableColumnInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let tbl = self
            .ctx
            .get_catalog(catalog_name)
            .await?
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await
            .ok();

        let table = if let Some(table) = &tbl {
            table
        } else {
            return Ok(PipelineBuildResult::create());
        };

        let table_info = table.get_table_info();
        if table_info.engine() == VIEW_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is VIEW that doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let table_meta = table.get_table_info().meta.clone();

        // NOTICE: if we support modify column data type,
        // need to check whether this column is referenced by other computed columns.
        match &self.plan.action {
            ModifyColumnAction::SetMaskingPolicy(column, mask_name) => {
                self.do_set_data_mask_policy(catalog, table, column.to_string(), mask_name.clone())
                    .await
            }
            ModifyColumnAction::UnsetMaskingPolicy(column) => {
                self.do_unset_data_mask_policy(catalog, table, column.to_string())
                    .await
            }
            ModifyColumnAction::SetDataType(column_name_types) => {
                self.do_set_data_type(table, column_name_types).await
            }
            ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                self.do_convert_stored_computed_column(
                    catalog,
                    table,
                    table_meta,
                    column.to_string(),
                )
                .await
            }
        }
    }
}
