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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license::Feature::DataMask;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::executor::physical_plans::DistributedInsertSelect;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::field_default_value;
use databend_common_sql::plans::ModifyColumnAction;
use databend_common_sql::plans::ModifyTableColumnPlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::Planner;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;

use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
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
        table: Arc<dyn Table>,
        column: String,
        mask_name: String,
    ) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), DataMask)?;

        if table.is_temp() {
            return Err(ErrorCode::StorageOther(format!(
                "Table {} is temporary table, setting data mask policy not allowed",
                table.name()
            )));
        }

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();
        let policy = handler
            .get_data_mask(meta_api, &self.ctx.get_tenant(), mask_name.clone())
            .await?;

        // check if column type match to the input type
        let policy_data_type = policy.args[0].1.to_string().to_lowercase();
        let schema = table.schema();
        let table_info = table.get_table_info();
        if let Some((_, data_field)) = schema.column_with_name(&column) {
            let data_type = data_field.data_type().to_string().to_lowercase();
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

        let _resp = catalog.set_table_column_mask_policy(req).await?;

        Ok(PipelineBuildResult::create())
    }

    // Set data column type.
    async fn do_set_data_type(
        &self,
        table: Arc<dyn Table>,
        field_and_comments: &[(TableField, String)],
    ) -> Result<PipelineBuildResult> {
        let schema = table.schema().as_ref().clone();
        let table_info = table.get_table_info();
        let mut new_schema = schema.clone();
        // first check default expr before lock table
        for (field, _comment) in field_and_comments {
            if let Some((i, old_field)) = schema.column_with_name(&field.name) {
                // if the field has different leaf column numbers, we need drop the old column
                // and add a new one to generate new column id. otherwise, leaf column ids will conflict.
                if old_field.data_type.num_leaf_columns() != field.data_type.num_leaf_columns() {
                    let _ = new_schema.drop_column(&field.name);
                    let _ = new_schema.add_column(field, i);
                } else {
                    // new field don't have `column_id`, assign field directly will cause `column_id` lost.
                    new_schema.fields[i].data_type = field.data_type.clone();
                    // TODO: support set computed field.
                    new_schema.fields[i].computed_expr = field.computed_expr.clone();
                }
                if let Some(default_expr) = &field.default_expr {
                    let default_expr = default_expr.to_string();
                    new_schema.fields[i].default_expr = Some(default_expr);
                    let _ = field_default_value(self.ctx.clone(), &new_schema.fields[i])?;
                } else {
                    new_schema.fields[i].default_expr = None;
                }
                if old_field.data_type != field.data_type {
                    // Check if this column is referenced by computed columns.
                    let data_schema = DataSchema::from(&new_schema);
                    check_referenced_computed_columns(
                        self.ctx.clone(),
                        Arc::new(data_schema),
                        &field.name,
                    )?;
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    field.name
                )));
            }
        }

        let catalog_name = table_info.catalog();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let base_snapshot = fuse_table.read_table_snapshot().await?;
        let prev_snapshot_id = base_snapshot.snapshot_id().map(|(id, _)| id);
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(fuse_table.get_id(), base_snapshot.clone())?;

        let mut bloom_index_cols = vec![];
        if let Some(v) = table_info.options().get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
            if let BloomIndexColumns::Specify(cols) = v.parse::<BloomIndexColumns>()? {
                bloom_index_cols = cols;
            }
        }

        let mut table_info = table.get_table_info().clone();
        table_info.meta.fill_field_comments();
        let mut modify_comment = false;
        for (field, comment) in field_and_comments {
            if let Some((i, old_field)) = schema.column_with_name(&field.name) {
                if old_field.data_type != field.data_type {
                    // If the column is defined in bloom index columns,
                    // check whether the data type is supported for bloom index.
                    if bloom_index_cols.iter().any(|v| v.as_str() == field.name)
                        && !BloomIndex::supported_type(&field.data_type)
                    {
                        return Err(ErrorCode::TableOptionInvalid(format!(
                            "Unsupported data type '{}' for bloom index",
                            field.data_type
                        )));
                    }
                    // If the column is inverted index column, the type can't be changed.
                    if !table_info.meta.indexes.is_empty() {
                        for (index_name, index) in &table_info.meta.indexes {
                            if index.column_ids.contains(&old_field.column_id)
                                && old_field.data_type.remove_nullable()
                                    != field.data_type.remove_nullable()
                            {
                                return Err(ErrorCode::ColumnReferencedByInvertedIndex(format!(
                                    "column `{}` is referenced by inverted index, drop inverted index `{}` first",
                                    field.name, index_name,
                                )));
                            }
                        }
                    }
                }
                if table_info.meta.field_comments[i] != *comment {
                    table_info.meta.field_comments[i] = comment.to_string();
                    modify_comment = true;
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    field.name
                )));
            }
        }

        // check if schema has changed
        if schema == new_schema && !modify_comment {
            return Ok(PipelineBuildResult::create());
        }

        let mut modified_field_indices = HashSet::new();
        let new_schema_without_computed_fields = new_schema.remove_computed_fields();
        if schema != new_schema {
            for (field, _) in field_and_comments {
                let field_index = new_schema_without_computed_fields.index_of(&field.name)?;
                let old_field = schema.field_with_name(&field.name)?;
                let is_alter_column_string_to_binary =
                    is_string_to_binary(&old_field.data_type, &field.data_type);
                // If two conditions are met, we don't need rebuild the table,
                // as rebuild table can be a time-consuming job.
                // 1. alter column from string to binary in parquet or data type not changed.
                // 2. default expr and computed expr not changed. Otherwise, we need fill value for
                //    new added column.
                if ((table.storage_format_as_parquet() && is_alter_column_string_to_binary)
                    || old_field.data_type.remove_nullable() == field.data_type.remove_nullable())
                    && old_field.default_expr == field.default_expr
                    && old_field.computed_expr == field.computed_expr
                {
                    continue;
                }
                modified_field_indices.insert(field_index);
            }
            table_info.meta.schema = new_schema.clone().into();
        }

        // if don't need rebuild table, only update table meta.
        if modified_field_indices.is_empty() {
            commit_table_meta(
                &self.ctx,
                table.as_ref(),
                &table_info,
                table_info.meta.clone(),
                catalog,
            )
            .await?;

            return Ok(PipelineBuildResult::create());
        }

        // construct sql for selecting data from old table.
        // computed columns are ignored, as it is build from other columns.
        let query_fields = new_schema_without_computed_fields
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if modified_field_indices.contains(&index) {
                    let old_field = schema.field_with_name(&field.name).unwrap();
                    // If the column type is Tuple or Array(Tuple), the difference in the number of leaf columns may cause
                    // the auto cast to fail.
                    // We read the leaf column data, and then use build function to construct a new Tuple or Array(Tuple).
                    // Note: other nested types auto cast can still fail, we need a more general handling
                    // to solve this problem in the future.
                    match (
                        old_field.data_type.remove_nullable(),
                        field.data_type.remove_nullable(),
                    ) {
                        (
                            TableDataType::Tuple {
                                fields_name: old_fields_name,
                                ..
                            },
                            TableDataType::Tuple {
                                fields_name: new_fields_name,
                                fields_type: new_fields_type,
                            },
                        ) => {
                            let transform_funcs = new_fields_name
                                .iter()
                                .zip(new_fields_type.iter())
                                .map(|(new_field_name, new_field_type)| {
                                    match old_fields_name.iter().position(|n| n == new_field_name) {
                                        Some(idx) => {
                                            format!("`{}`.{}", field.name, idx + 1)
                                        }
                                        None => {
                                            let new_data_type = DataType::from(new_field_type);
                                            let default_value =
                                                Scalar::default_value(&new_data_type);
                                            format!("{default_value}")
                                        }
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ");

                            format!(
                                "if(is_not_null(`{}`), tuple({}), NULL) AS {}",
                                field.name, transform_funcs, field.name
                            )
                        }
                        (
                            TableDataType::Array(box TableDataType::Tuple {
                                fields_name: old_fields_name,
                                ..
                            }),
                            TableDataType::Array(box TableDataType::Tuple {
                                fields_name: new_fields_name,
                                fields_type: new_fields_type,
                            }),
                        )
                        | (
                            TableDataType::Array(box TableDataType::Nullable(
                                box TableDataType::Tuple {
                                    fields_name: old_fields_name,
                                    ..
                                },
                            )),
                            TableDataType::Array(box TableDataType::Tuple {
                                fields_name: new_fields_name,
                                fields_type: new_fields_type,
                            }),
                        )
                        | (
                            TableDataType::Array(box TableDataType::Tuple {
                                fields_name: old_fields_name,
                                ..
                            }),
                            TableDataType::Array(box TableDataType::Nullable(
                                box TableDataType::Tuple {
                                    fields_name: new_fields_name,
                                    fields_type: new_fields_type,
                                },
                            )),
                        )
                        | (
                            TableDataType::Array(box TableDataType::Nullable(
                                box TableDataType::Tuple {
                                    fields_name: old_fields_name,
                                    ..
                                },
                            )),
                            TableDataType::Array(box TableDataType::Nullable(
                                box TableDataType::Tuple {
                                    fields_name: new_fields_name,
                                    fields_type: new_fields_type,
                                },
                            )),
                        ) => {
                            let transform_funcs = new_fields_name
                                .iter()
                                .zip(new_fields_type.iter())
                                .map(|(new_field_name, new_field_type)| {
                                    match old_fields_name.iter().position(|n| n == new_field_name) {
                                        Some(idx) => {
                                            format!(
                                                "array_transform(`{}`, v -> v.{})",
                                                field.name,
                                                idx + 1
                                            )
                                        }
                                        None => {
                                            let new_data_type = DataType::from(new_field_type);
                                            let default_value =
                                                Scalar::default_value(&new_data_type);
                                            format!("{default_value}")
                                        }
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ");

                            format!(
                                "if(is_not_null(`{}`), arrays_zip({}), NULL) AS {}",
                                field.name, transform_funcs, field.name
                            )
                        }
                        (_, _) => {
                            format!("`{}`", field.name)
                        }
                    }
                } else {
                    format!("`{}`", field.name)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT {} FROM `{}`.`{}`",
            query_fields, self.plan.database, self.plan.table
        );

        build_select_insert_plan(
            self.ctx.clone(),
            sql,
            table_info,
            new_schema_without_computed_fields.into(),
            prev_snapshot_id,
            table_meta_timestamps,
        )
        .await
    }

    // unset data mask policy to a column is a ee feature.
    async fn do_unset_data_mask_policy(
        &self,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        column: String,
    ) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), DataMask)?;

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

            let _resp = catalog.set_table_column_mask_policy(req).await?;
        }

        Ok(PipelineBuildResult::create())
    }

    async fn do_convert_stored_computed_column(
        &self,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        table_meta: TableMeta,
        column: String,
    ) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;

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
        };

        let _resp = catalog.update_single_table_meta(req, table_info).await?;

        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for ModifyTableColumnInterpreter {
    fn name(&self) -> &str {
        "ModifyTableColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let table = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        table.check_mutable()?;

        let table_info = table.get_table_info();
        let engine = table.engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                db_name, tbl_name, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                db_name, tbl_name
            )));
        }

        let table_meta = table.get_table_info().meta.clone();

        // NOTICE: if we support modify column data type,
        // need to check whether this column is referenced by other computed columns.
        let mut build_res = match &self.plan.action {
            ModifyColumnAction::SetMaskingPolicy(column, mask_name) => {
                self.do_set_data_mask_policy(catalog, table, column.to_string(), mask_name.clone())
                    .await?
            }
            ModifyColumnAction::UnsetMaskingPolicy(column) => {
                self.do_unset_data_mask_policy(catalog, table, column.to_string())
                    .await?
            }
            ModifyColumnAction::SetDataType(field_and_comment) => {
                self.do_set_data_type(table, field_and_comment).await?
            }
            ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                self.do_convert_stored_computed_column(
                    catalog,
                    table,
                    table_meta,
                    column.to_string(),
                )
                .await?
            }
        };

        build_res
            .main_pipeline
            .add_lock_guard(self.plan.lock_guard.clone());
        Ok(build_res)
    }
}

fn is_string_to_binary(old_ty: &TableDataType, new_ty: &TableDataType) -> bool {
    match (old_ty, new_ty) {
        (TableDataType::String, TableDataType::Binary) => true,
        (TableDataType::Nullable(old_ty), TableDataType::Nullable(new_ty)) => {
            is_string_to_binary(old_ty, new_ty)
        }
        (TableDataType::Map(old_ty), TableDataType::Map(new_ty)) => {
            is_string_to_binary(old_ty, new_ty)
        }
        (TableDataType::Array(old_ty), TableDataType::Array(new_ty)) => {
            is_string_to_binary(old_ty, new_ty)
        }
        (
            TableDataType::Tuple {
                fields_type: old_tys,
                ..
            },
            TableDataType::Tuple {
                fields_type: new_tys,
                ..
            },
        ) => {
            old_tys.len() == new_tys.len()
                && old_tys
                    .iter()
                    .zip(new_tys)
                    .all(|(old_ty, new_ty)| is_string_to_binary(old_ty, new_ty))
        }
        _ => false,
    }
}

pub(crate) async fn build_select_insert_plan(
    ctx: Arc<QueryContext>,
    sql: String,
    table_info: TableInfo,
    new_schema: TableSchemaRef,
    prev_snapshot_id: Option<SnapshotId>,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<PipelineBuildResult> {
    // 1. build plan by sql
    let mut planner = Planner::new(ctx.clone());
    let (plan, _extras) = planner.plan_sql(&sql).await?;
    let select_schema = plan.schema();

    // 2. build physical plan by plan
    let (select_plan, select_column_bindings) = match plan {
        Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } => {
            let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
            (
                builder.build(&s_expr, bind_context.column_set()).await?,
                bind_context.columns.clone(),
            )
        }
        _ => unreachable!(),
    };

    // 3. define select schema and insert schema of DistributedInsertSelect plan
    let new_table = FuseTable::try_create(table_info)?;

    // 4. build DistributedInsertSelect plan
    let insert_plan = PhysicalPlan::DistributedInsertSelect(Box::new(DistributedInsertSelect {
        plan_id: select_plan.get_id(),
        input: Box::new(select_plan),
        table_info: new_table.get_table_info().clone(),
        select_schema,
        select_column_bindings,
        insert_schema: Arc::new(new_schema.into()),
        cast_needed: true,
        table_meta_timestamps,
    }));
    let mut build_res = build_query_pipeline_without_render_result_set(&ctx, &insert_plan).await?;

    // 5. commit new meta schema and snapshots
    new_table.commit_insertion(
        ctx.clone(),
        &mut build_res.main_pipeline,
        None,
        vec![],
        true,
        prev_snapshot_id,
        None,
        table_meta_timestamps,
    )?;

    Ok(build_res)
}
