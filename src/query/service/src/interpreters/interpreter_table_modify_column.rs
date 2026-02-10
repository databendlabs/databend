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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::ExtendedTableInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license::Feature::DataMask;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::schema::BranchInfo;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::SetSecurityPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::ApproxDistinctColumns;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::DefaultExprBinder;
use databend_common_sql::Planner;
use databend_common_sql::analyze_cluster_keys;
use databend_common_sql::plans::ModifyColumnAction;
use databend_common_sql::plans::ModifyTableColumnPlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::resolve_type_name_by_str;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use databend_meta_types::MatchSeq;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_APPROX_DISTINCT_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;

use crate::interpreters::Interpreter;
use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::common::cluster_key_referenced_columns;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::meta_service_error;
use crate::physical_plans::DistributedInsertSelect;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
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
        using_columns: &[String],
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

        // Get mask policy ID from name using KV API
        let name_ident = DataMaskNameIdent::new(self.ctx.get_tenant(), mask_name.clone());
        let mask_id_seq = meta_api
            .get_pb(&name_ident)
            .await
            .map_err(meta_service_error)?;
        let policy_id = match mask_id_seq {
            Some(seq_id) => seq_id.data,
            None => {
                return Err(ErrorCode::UnknownDatamask(format!(
                    "Data mask policy {} not found",
                    mask_name
                )));
            }
        };

        let policy = handler
            .get_data_mask(meta_api, &self.ctx.get_tenant(), mask_name.clone())
            .await?;

        // check if column type match to the input type - similar to row access policy validation
        let policy_data_types: Result<Vec<_>> = policy
            .args
            .iter()
            .map(|(_, type_str)| {
                let table_data_type = resolve_type_name_by_str(type_str, false)?;
                Ok(table_data_type.remove_nullable())
            })
            .collect();
        let policy_data_types = policy_data_types?;

        let schema = table.schema();
        let table_info = table.get_table_info();

        if using_columns.len() != policy_data_types.len() {
            return Err(ErrorCode::UnmatchColumnDataType(format!(
                "Number of columns ({}) does not match the number of mask policy arguments ({})",
                using_columns.len(),
                policy_data_types.len()
            )));
        }

        let mut columns_ids = Vec::with_capacity(using_columns.len());
        for (column, policy_data_type) in using_columns.iter().zip(policy_data_types) {
            let (_, data_field) = schema.column_with_name(column).ok_or_else(|| {
                ErrorCode::UnknownColumn(format!("Cannot find column {}", column))
            })?;

            if table_info
                .meta
                .is_column_reference_policy(&data_field.column_id)
            {
                return Err(ErrorCode::AlterTableError(format!(
                    "Column '{}' is already attached to a security policy. A column cannot be attached to multiple security policies",
                    data_field.name
                )));
            }

            let column_type = data_field.data_type();
            if policy_data_type != column_type.remove_nullable() {
                return Err(ErrorCode::UnmatchColumnDataType(format!(
                    "Column '{}' data type {} does not match to the mask policy type {}",
                    column, column_type, policy_data_type,
                )));
            }

            columns_ids.push(data_field.column_id);
        }

        let table_id = table_info.ident.table_id;

        let req = SetTableColumnMaskPolicyReq {
            tenant: self.ctx.get_tenant(),
            seq: MatchSeq::Exact(table_info.ident.seq),
            table_id,
            action: SetSecurityPolicyAction::Set(*policy_id, columns_ids),
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
        let schema = table.schema();
        let table_info = table.get_table_info();
        let mut new_schema = schema.as_ref().clone();
        let mut modified_cols = HashSet::with_capacity(field_and_comments.len());
        // first check default expr before lock table
        for (field, _comment) in field_and_comments {
            if let Some((i, old_field)) = schema.column_with_name(&field.name) {
                // if the field has different leaf column numbers, we need drop the old column
                // and add a new one to generate new column id. otherwise, leaf column ids will conflict.
                if old_field.data_type.num_leaf_columns() != field.data_type.num_leaf_columns() {
                    let _ = new_schema.drop_column(&field.name)?;
                    new_schema.add_column(field, i)?;
                } else {
                    // new field don't have `column_id`, assign field directly will cause `column_id` lost.
                    new_schema.fields[i].data_type = field.data_type.clone();
                    // TODO: support set computed field.
                    new_schema.fields[i].computed_expr = field.computed_expr.clone();
                }

                if let Some(default_expr) = &field.default_expr {
                    let default_expr = default_expr.to_string();
                    new_schema.fields[i].default_expr = Some(default_expr);
                } else {
                    new_schema.fields[i].default_expr = None;
                }

                if old_field.data_type != field.data_type {
                    modified_cols.insert(field.name.clone());
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

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let new_schema = Arc::new(new_schema);
        if !modified_cols.is_empty() {
            // Only need to validate the data types of modified columns that are referenced
            // by the cluster key. The cluster key expression itself is already validated
            // when it is created or altered, so we must NOT re-check the expression type here.
            if let Some((_, cluster_key)) = table.cluster_key_meta() {
                let referenced = cluster_key_referenced_columns(&cluster_key)?;
                if referenced.iter().any(|v| modified_cols.contains(v)) {
                    let tmp_table = fuse_table.with_schema(new_schema.clone());
                    if let Err(e) = analyze_cluster_keys(self.ctx.clone(), tmp_table, &cluster_key)
                    {
                        return Err(ErrorCode::AlterTableError(format!(
                            "Cannot modify column data type, because it is referenced by cluster key '{}': {}",
                            cluster_key,
                            e.message()
                        )));
                    }
                }
            }
        }

        let catalog_name = table_info.catalog();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let base_snapshot = fuse_table.read_table_snapshot().await?;
        let prev_snapshot_id = base_snapshot.snapshot_id().map(|(id, _)| id);
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(table.as_ref(), base_snapshot.clone())?;

        let mut bloom_index_cols = vec![];
        let mut approx_distinct_cols = vec![];
        if self.plan.branch.is_none() {
            if let Some(v) = table_info.options().get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
                if let BloomIndexColumns::Specify(cols) = v.parse::<BloomIndexColumns>()? {
                    bloom_index_cols = cols;
                }
            }

            if let Some(v) = table_info.options().get(OPT_KEY_APPROX_DISTINCT_COLUMNS) {
                if let ApproxDistinctColumns::Specify(cols) = v.parse::<ApproxDistinctColumns>()? {
                    approx_distinct_cols = cols;
                }
            }
        }

        let mut table_info = table.get_table_info().clone();
        table_info.meta.fill_field_comments();
        let mut modify_comment = false;
        for (field, comment) in field_and_comments {
            if let Some((i, old_field)) = schema.column_with_name(&field.name) {
                if table_info
                    .meta
                    .is_column_reference_policy(&old_field.column_id)
                {
                    return Err(ErrorCode::AlterTableError(format!(
                        "Cannot modify column '{}' which is associated with a security policy",
                        old_field.name
                    )));
                }

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
                    if approx_distinct_cols
                        .iter()
                        .any(|v| v.as_str() == field.name)
                        && !RangeIndex::supported_table_type(&field.data_type)
                    {
                        return Err(ErrorCode::TableOptionInvalid(format!(
                            "Unsupported data type '{}' for approx distinct columns",
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

                // Ignore column comment modify for table branch.
                if self.plan.branch.is_none() && table_info.meta.field_comments[i] != *comment {
                    table_info.meta.field_comments[i] = comment.to_string();
                    modify_comment = true;
                }

                // Check for NULL values in columns that are being changed to NOT NULL
                if old_field.data_type.is_nullable() && !field.data_type.is_nullable() {
                    let statistics_provider = fuse_table
                        .column_statistics_provider(self.ctx.clone())
                        .await?;
                    let column_stat = statistics_provider
                        .column_statistics(old_field.column_id)
                        .ok_or_else(|| {
                            ErrorCode::UnknownColumn(format!(
                                "Cannot find statistics for column '{}' (id: {})",
                                field.name, old_field.column_id
                            ))
                        })?;
                    if column_stat.null_count > 0 {
                        return Err(ErrorCode::BadArguments(format!(
                            "Cannot change column '{}' to NOT NULL: contains {} NULL values",
                            field.name, column_stat.null_count
                        )));
                    }
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

        let mut modified_default_scalars = HashMap::new();
        let mut default_expr_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
        let new_schema_without_computed_fields = new_schema.remove_computed_fields();
        let format_as_parquet = fuse_table.storage_format_as_parquet();
        if schema != new_schema {
            for (field, _) in field_and_comments {
                let old_field = schema.field_with_name(&field.name)?;
                let is_alter_column_string_to_binary =
                    is_string_to_binary(&old_field.data_type, &field.data_type);
                // If two conditions are met, we don't need rebuild the table,
                // as rebuild table can be a time-consuming job.
                // 1. alter column from string to binary in parquet or data type not changed.
                // 2. default expr and computed expr not changed. Otherwise, we need fill value for
                //    new added column.
                if ((format_as_parquet && is_alter_column_string_to_binary)
                    || old_field.data_type == field.data_type)
                    && old_field.default_expr == field.default_expr
                    && old_field.computed_expr == field.computed_expr
                {
                    continue;
                }
                let field_index = new_schema_without_computed_fields.index_of(&field.name)?;
                let default_scalar = default_expr_binder
                    .get_scalar(&new_schema_without_computed_fields.fields[field_index])?;
                modified_default_scalars.insert(field_index, default_scalar);
            }
        }

        // if don't need to rebuild table, only update table meta.
        if modified_default_scalars.is_empty()
            || base_snapshot.is_none_or(|v| v.summary.row_count == 0)
        {
            commit_table_meta(
                &self.ctx,
                table.as_ref(),
                table_info.meta.clone(),
                catalog,
                |snapshot_opt, meta| {
                    if let Some(snapshot) = snapshot_opt {
                        snapshot.schema = new_schema.as_ref().clone();
                    }
                    if self.plan.branch.is_none() {
                        meta.schema = new_schema.clone();
                    }
                },
            )
            .await?;

            return Ok(PipelineBuildResult::create());
        }

        if fuse_table.change_tracking_enabled() {
            // Modifying columns while change tracking is active may break
            // the consistency between tracked changes and the current table schema,
            // leading to incorrect or incomplete change records.
            return Err(ErrorCode::AlterTableError(format!(
                "table {} has change tracking enabled, modifying columns should be avoided",
                table_info.desc
            )));
        }

        // construct sql for selecting data from old table.
        // computed columns are ignored, as it is build from other columns.
        let query_fields = new_schema_without_computed_fields
            .fields()
            .iter()
            .map(|field| {
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
                                        let default_value = Scalar::default_value(&new_data_type);
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
                                        let default_value = Scalar::default_value(&new_data_type);
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
                        // No need to remove_nullable already check NULL value
                        format!("`{}`", field.name)
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let table_ref = if let Some(branch) = &self.plan.branch {
            format!(
                "`{}`.`{}`/`{}`",
                self.plan.database, self.plan.table, branch
            )
        } else {
            format!("`{}`.`{}`", self.plan.database, self.plan.table)
        };
        let sql = format!("SELECT {} FROM {}", query_fields, table_ref);

        let mut branch_info = fuse_table.get_branch_info().cloned();
        if let Some(inner) = branch_info.as_mut() {
            inner.schema = new_schema;
        } else {
            table_info.meta.schema = new_schema;
        }

        build_select_insert_plan(
            self.ctx.clone(),
            sql,
            table_info,
            branch_info,
            new_schema_without_computed_fields.into(),
            prev_snapshot_id,
            table_meta_timestamps,
        )
        .await
    }

    // Set column comment.
    async fn do_set_comment(
        &self,
        table: Arc<dyn Table>,
        field_and_comments: &[(TableField, String)],
    ) -> Result<PipelineBuildResult> {
        let schema = table.schema().as_ref().clone();
        let table_info = table.get_table_info();

        let catalog_name = table_info.catalog();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let mut new_table_meta = table.get_table_info().meta.clone();
        new_table_meta.fill_field_comments();
        let mut modify_comment = false;
        for (field, comment) in field_and_comments {
            if let Some((i, _)) = schema.column_with_name(&field.name) {
                if new_table_meta.field_comments[i] != *comment {
                    new_table_meta.field_comments[i] = comment.to_string();
                    modify_comment = true;
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    field.name
                )));
            }
        }

        if modify_comment {
            commit_table_meta(
                &self.ctx,
                table.as_ref(),
                new_table_meta,
                catalog,
                |_, _| {},
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
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
        let column_id = table_info
            .schema()
            .fields()
            .iter()
            .find(|field| field.name.as_str() == column.as_str())
            .map(|field| field.column_id)
            .ok_or_else(|| ErrorCode::UnknownColumn(format!("Cannot find column {}", column)))?;

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        if let Some(policy) = table_info
            .meta
            .column_mask_policy_columns_ids
            .get(&column_id)
        {
            let req = SetTableColumnMaskPolicyReq {
                tenant: self.ctx.get_tenant(),
                seq: MatchSeq::Exact(table_version),
                table_id,
                action: SetSecurityPolicyAction::Unset(policy.policy_id),
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

        commit_table_meta(
            &self.ctx,
            table.as_ref(),
            table_meta,
            catalog,
            |snapshot_opt, meta| {
                if let Some(snapshot) = snapshot_opt {
                    snapshot.schema = new_schema.clone();
                }
                if self.plan.branch.is_none() {
                    meta.schema = Arc::new(new_schema);
                }
            },
        )
        .await?;
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
        let table = catalog
            .get_table_with_branch(
                &self.ctx.get_tenant(),
                db_name,
                tbl_name,
                self.plan.branch.as_deref(),
            )
            .await?;

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

        if self.plan.branch.is_some()
            && !matches!(
                self.plan.action,
                ModifyColumnAction::SetDataType { .. }
                    | ModifyColumnAction::ConvertStoredComputedColumn { .. }
            )
        {
            return Err(ErrorCode::SemanticError(
                "ALTER TABLE <table>/<branch> MODIFY COLUMN only supports MODIFY TYPE, DROP STORED",
            ));
        }

        // NOTICE: if we support modify column data type,
        // need to check whether this column is referenced by other computed columns.
        let mut build_res = match &self.plan.action {
            ModifyColumnAction::SetMaskingPolicy(mask_name, using_columns) => {
                self.do_set_data_mask_policy(catalog, table, using_columns, mask_name.clone())
                    .await?
            }
            ModifyColumnAction::UnsetMaskingPolicy(column) => {
                self.do_unset_data_mask_policy(catalog, table, column.to_string())
                    .await?
            }
            ModifyColumnAction::SetDataType(field_and_comment) => {
                self.do_set_data_type(table, field_and_comment).await?
            }
            ModifyColumnAction::Comment(field_and_comment) => {
                self.do_set_comment(table, field_and_comment).await?
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

        let lock_guard = self
            .plan
            .lock_guard
            .as_ref()
            .and_then(|holder| holder.try_take());
        build_res.main_pipeline.add_lock_guard(lock_guard);
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
    branch_info: Option<BranchInfo>,
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
    let new_table = FuseTable::create_and_refresh_table_info(
        table_info,
        branch_info.clone(),
        ctx.get_settings().get_s3_storage_class()?,
    )?;

    // 4. build DistributedInsertSelect plan
    let mut insert_plan = PhysicalPlan::new(DistributedInsertSelect {
        input: select_plan,
        table_info: ExtendedTableInfo {
            table_info: new_table.get_table_info().clone(),
            branch_info,
        },
        select_schema,
        select_column_bindings,
        insert_schema: Arc::new(new_schema.into()),
        cast_needed: true,
        table_meta_timestamps,
        meta: PhysicalPlanMeta::new("DistributedInsertSelect"),
    });

    let mut index = 0;
    insert_plan.adjust_plan_id(&mut index);
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
