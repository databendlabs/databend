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

use arrow_schema::Schema as ArrowSchema;
use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license::Feature::DataMask;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
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
use databend_common_sql::binder::validate_constraints_by_schema;
use databend_common_sql::parse_cluster_keys;
use databend_common_sql::plans::ModifyColumnAction;
use databend_common_sql::plans::ModifyTableColumnPlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::resolve_type_name_by_str;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::CachedMetaWriter;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::read_segment_stats;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_ANALYZE_FREQUENCY_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_APPROX_DISTINCT_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_PARTITION_BY;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use parquet::arrow::ArrowSchemaConverter;

use crate::interpreters::Interpreter;
use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::common::cluster_key_referenced_columns;
use crate::interpreters::common::stored_computed_column_references;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::interpreters::interpreter_table_add_column::update_table_meta;
use crate::meta_service_error;
use crate::physical_plans::DistributedInsertSelect;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub struct ModifyTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: ModifyTableColumnPlan,
}

#[derive(Clone, Copy)]
struct DecimalStatsRewrite {
    column_id: u32,
    size: DecimalSize,
}

#[derive(Clone, Copy)]
struct DecimalClusterStatsRewrite {
    cluster_key_id: u32,
    dimension: usize,
    size: DecimalSize,
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
        let mut modified_column_ids = HashSet::new();
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
                    // Aggregating indexes still bind to the existing table column ids, so
                    // MODIFY COLUMN must check the ids from the current schema instead of the
                    // freshly analyzed field definition.
                    modified_column_ids.extend(old_field.column_ids());
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
            if let Some(partition_key) = table.options().get(OPT_KEY_PARTITION_BY) {
                let referenced = cluster_key_referenced_columns(partition_key)?;
                if referenced.iter().any(|v| modified_cols.contains(v)) {
                    return Err(ErrorCode::AlterTableError(format!(
                        "Cannot modify column data type because it is referenced by partition key '{}'",
                        partition_key
                    )));
                }
            }
        }

        let catalog_name = table_info.catalog();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        validate_constraints_by_schema(
            self.ctx.clone(),
            &table_info.meta.constraints,
            new_schema.as_ref(),
        )?;

        let base_snapshot = fuse_table.read_table_snapshot().await?;
        let prev_snapshot_id = base_snapshot.snapshot_id().map(|(id, _)| id);
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(table.as_ref(), base_snapshot.clone())?;

        let mut bloom_index_cols = vec![];
        if let Some(v) = table_info.options().get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
            if let BloomIndexColumns::Specify(cols) = v.parse::<BloomIndexColumns>()? {
                bloom_index_cols = cols;
            }
        }

        let mut approx_distinct_cols = vec![];
        if let Some(v) = table_info.options().get(OPT_KEY_APPROX_DISTINCT_COLUMNS) {
            if let ApproxDistinctColumns::Specify(cols) = v.parse::<ApproxDistinctColumns>()? {
                approx_distinct_cols = cols;
            }
        }
        let mut analyze_frequency_cols = vec![];
        if let Some(v) = table_info.options().get(OPT_KEY_ANALYZE_FREQUENCY_COLUMNS) {
            if let ApproxDistinctColumns::Specify(cols) = v.parse::<ApproxDistinctColumns>()? {
                analyze_frequency_cols = cols;
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
                    if analyze_frequency_cols
                        .iter()
                        .any(|v| v.as_str() == field.name)
                        && !RangeIndex::supported_table_type(&field.data_type)
                    {
                        return Err(ErrorCode::TableOptionInvalid(format!(
                            "Unsupported data type '{}' for analyze frequency columns",
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
                                return Err(ErrorCode::ColumnReferencedByIndex(format!(
                                    "column `{}` is referenced by {} index, drop index `{}` first",
                                    field.name, index.index_type, index_name,
                                )));
                            }
                        }
                    }
                }

                // Ignore column comment modify for table branch.
                if table_info.meta.field_comments[i] != *comment {
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
        let mut decimal_stats_rewrites = Vec::new();
        let mut default_expr_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
        let new_schema_without_computed_fields = new_schema.remove_computed_fields();
        let new_data_schema = Arc::new(DataSchema::from(new_schema.as_ref()));
        let format_as_parquet = fuse_table.storage_format_as_parquet();
        if schema != new_schema {
            for (field, _) in field_and_comments {
                let old_field = schema.field_with_name(&field.name)?;
                let is_alter_column_string_to_binary =
                    is_string_to_binary(&old_field.data_type, &field.data_type);
                let is_decimal_precision_widening = format_as_parquet
                    && !fuse_table.is_column_oriented()
                    && is_decimal_precision_widening(&old_field.data_type, &field.data_type)?;
                let has_stored_computed_dependency = is_decimal_precision_widening
                    && stored_computed_column_references(
                        self.ctx.clone(),
                        new_data_schema.clone(),
                        &field.name,
                    )?;
                let is_metadata_only_decimal_widening =
                    is_decimal_precision_widening && !has_stored_computed_dependency;
                if is_metadata_only_decimal_widening {
                    decimal_stats_rewrites.push(DecimalStatsRewrite {
                        column_id: old_field.column_id,
                        size: decimal_size(&field.data_type).ok_or_else(|| {
                            ErrorCode::Internal("Decimal widening has no target DecimalSize")
                        })?,
                    });
                }
                // If two conditions are met, we don't need rebuild the table,
                // as rebuild table can be a time-consuming job.
                // 1. alter column from string to binary in parquet or data type not changed.
                // 2. default expr and computed expr not changed. Otherwise, we need fill value for
                //    new added column.
                if ((format_as_parquet && is_alter_column_string_to_binary)
                    || is_metadata_only_decimal_widening
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

        let table_is_empty = base_snapshot
            .as_ref()
            .is_none_or(|snapshot| snapshot.summary.row_count == 0);
        let has_persisted_segments = base_snapshot
            .as_ref()
            .is_some_and(|snapshot| !snapshot.segments.is_empty());
        // If this defensive state ever occurs, Decimal statistics in the remaining segment
        // objects still need retagging. Keep this segment-based gate scoped to Decimal so the
        // existing empty-table behavior of unrelated metadata-only alters does not change.
        let needs_decimal_stats_rewrite = !decimal_stats_rewrites.is_empty()
            && has_persisted_segments
            && !fuse_table.is_column_oriented();
        let decimal_cluster_rewrites =
            if modified_default_scalars.is_empty() && needs_decimal_stats_rewrite {
                decimal_cluster_stats_rewrites(self.ctx.clone(), fuse_table, new_schema.clone())?
            } else {
                Some(Vec::new())
            };

        // if don't need to rebuild table, only update table meta.
        if (modified_default_scalars.is_empty() && decimal_cluster_rewrites.is_some())
            || (table_is_empty && !needs_decimal_stats_rewrite)
        {
            if needs_decimal_stats_rewrite {
                let cluster_rewrites = decimal_cluster_rewrites.as_deref().unwrap_or_default();
                rewrite_decimal_stats_and_commit(
                    &self.ctx,
                    fuse_table,
                    base_snapshot.unwrap(),
                    new_schema,
                    table_info.meta.clone(),
                    catalog,
                    &decimal_stats_rewrites,
                    cluster_rewrites,
                    table_meta_timestamps,
                )
                .await?;
                return Ok(PipelineBuildResult::create());
            }
            commit_table_meta(
                &self.ctx,
                table.as_ref(),
                table_info.meta.clone(),
                catalog,
                |snapshot_opt, meta| {
                    if let Some(snapshot) = snapshot_opt {
                        snapshot.schema = new_schema.as_ref().clone();
                    }
                    meta.schema = new_schema.clone();
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
        table_info.meta.schema = new_schema;

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
                meta.schema = Arc::new(new_schema);
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
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
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
        })
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

fn is_decimal_precision_widening(old_ty: &TableDataType, new_ty: &TableDataType) -> Result<bool> {
    match (old_ty, new_ty) {
        (TableDataType::Decimal(old), TableDataType::Decimal(new)) => {
            if old.scale() != new.scale() || old.precision() >= new.precision() {
                return Ok(false);
            }
            Ok(decimal_parquet_physical_type(old_ty)? == decimal_parquet_physical_type(new_ty)?)
        }
        (TableDataType::Nullable(old), TableDataType::Nullable(new)) => {
            is_decimal_precision_widening(old, new)
        }
        _ => Ok(false),
    }
}

fn decimal_size(data_type: &TableDataType) -> Option<DecimalSize> {
    match data_type {
        TableDataType::Decimal(decimal) => Some(decimal.size()),
        TableDataType::Nullable(inner) => decimal_size(inner),
        _ => None,
    }
}

fn expression_decimal_size(data_type: &DataType) -> Option<DecimalSize> {
    match data_type {
        DataType::Decimal(size) => Some(*size),
        DataType::Nullable(inner) => expression_decimal_size(inner),
        _ => None,
    }
}

fn decimal_cluster_stats_rewrites(
    ctx: Arc<QueryContext>,
    table: &FuseTable,
    new_schema: TableSchemaRef,
) -> Result<Option<Vec<DecimalClusterStatsRewrite>>> {
    let Some((cluster_key_id, _)) = table.cluster_key_meta() else {
        return Ok(Some(Vec::new()));
    };
    let ast_exprs = table
        .resolve_cluster_keys()
        .expect("cluster key metadata was checked above");
    let old_keys = parse_cluster_keys(ctx.clone(), Arc::new(table.clone()), ast_exprs.clone())?;
    // Hilbert cluster statistics persist MBR dimensions rather than ordinary key-expression
    // positions. Fall back to the existing table rewrite instead of trying to retag them.
    if old_keys.is_hilbert() {
        return Ok(None);
    }
    let new_table = table.with_schema(new_schema);
    let new_keys = parse_cluster_keys(ctx, new_table, ast_exprs)?;
    if new_keys.is_hilbert() {
        return Ok(None);
    }
    // `into_stats_keys` mirrors the writer layout and removes a vector key, which is not
    // persisted in ClusterStatistics min/max.
    let old_exprs = old_keys.into_stats_keys();
    let new_exprs = new_keys.into_stats_keys();
    if old_exprs.len() != new_exprs.len() {
        return Ok(None);
    }

    let mut rewrites = Vec::new();
    for (index, (old, new)) in old_exprs.iter().zip(&new_exprs).enumerate() {
        // A derived expression can depend on type metadata even when its result type is
        // unchanged (for example, `concat(typeof(d), to_string(d))`). If any referenced
        // column is rebound with a different type, only a direct reference to that same column
        // is known to preserve values; all other expressions must use the table-rewrite path.
        if old.column_refs() != new.column_refs()
            && !matches!(
                (old, new),
                (Expr::ColumnRef(old_ref), Expr::ColumnRef(new_ref))
                    if old_ref.id == new_ref.id
            )
        {
            return Ok(None);
        }
        if old.data_type() == new.data_type() {
            continue;
        }
        let (Some(old_size), Some(new_size)) = (
            expression_decimal_size(old.data_type()),
            expression_decimal_size(new.data_type()),
        ) else {
            return Ok(None);
        };
        if old_size.scale() != new_size.scale()
            || old_size.data_kind() != new_size.data_kind()
            || old_size.precision() > new_size.precision()
        {
            return Ok(None);
        }
        rewrites.push(DecimalClusterStatsRewrite {
            cluster_key_id,
            dimension: index,
            size: new_size,
        });
    }
    Ok(Some(rewrites))
}

fn widen_cluster_statistics(
    stats: &mut Option<databend_storages_common_table_meta::meta::ClusterStatistics>,
    rewrites: &[DecimalClusterStatsRewrite],
) -> Result<()> {
    if let Some(stats) = stats {
        for rewrite in rewrites {
            // Blocks written before ALTER TABLE ... CLUSTER BY retain statistics for the old
            // key. Their dimensions must never be interpreted using the current key layout.
            if stats.cluster_key_id == rewrite.cluster_key_id {
                stats.widen_decimal_dimension(rewrite.dimension, rewrite.size)?;
            }
        }
    }
    Ok(())
}

fn widen_segment_metadata(
    segment: &mut SegmentInfo,
    column_rewrites: &[DecimalStatsRewrite],
    cluster_rewrites: &[DecimalClusterStatsRewrite],
) -> Result<()> {
    for block in &mut segment.blocks {
        let block = Arc::make_mut(block);
        for rewrite in column_rewrites {
            if let Some(stats) = block.col_stats.get_mut(&rewrite.column_id) {
                stats.widen_decimal_size(rewrite.size)?;
            }
        }
        widen_cluster_statistics(&mut block.cluster_stats, cluster_rewrites)?;
    }
    for rewrite in column_rewrites {
        segment
            .summary
            .widen_decimal_column(rewrite.column_id, rewrite.size)?;
    }
    widen_cluster_statistics(&mut segment.summary.cluster_stats, cluster_rewrites)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn rewrite_decimal_stats_and_commit(
    ctx: &Arc<QueryContext>,
    fuse_table: &FuseTable,
    base_snapshot: Arc<TableSnapshot>,
    new_schema: TableSchemaRef,
    mut new_table_meta: TableMeta,
    catalog: Arc<dyn Catalog>,
    column_rewrites: &[DecimalStatsRewrite],
    cluster_rewrites: &[DecimalClusterStatsRewrite],
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<()> {
    const SEGMENT_READ_CHUNK_SIZE: usize = 1000;

    let operator = fuse_table.get_operator_ref();
    let segments_io = SegmentsIO::create(ctx.clone(), operator.clone(), fuse_table.schema());
    let mut new_segment_locations = Vec::with_capacity(base_snapshot.segments.len());

    for chunk in base_snapshot.segments.chunks(SEGMENT_READ_CHUNK_SIZE) {
        let segments = segments_io
            .read_segments::<SegmentInfo>(chunk, false)
            .await?;
        for segment in segments {
            let mut segment = segment?;
            widen_segment_metadata(&mut segment, column_rewrites, cluster_rewrites)?;

            // Preserve Vacuum2's ordering invariant: segment object UUID timestamps must be
            // derived from the snapshot's monotonically increased metadata timestamp. Each
            // call still receives fresh random UUID bits and therefore produces a unique key.
            let segment_location = fuse_table
                .meta_location_generator()
                .gen_segment_info_location(table_meta_timestamps, false);
            if let Some(old_stats_location) = segment.summary.additional_stats_loc() {
                let mut stats = read_segment_stats(operator.clone(), old_stats_location)
                    .await?
                    .as_ref()
                    .clone();
                for rewrite in column_rewrites {
                    stats.widen_decimal_column(rewrite.column_id, rewrite.size)?;
                }
                let stats_location = databend_common_storages_fuse::io::TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                    &segment_location,
                );
                let stats_size = stats.to_bytes()?.len() as u64;
                stats.write_meta(operator, &stats_location).await?;
                if let Some(meta) = &mut segment.summary.additional_stats_meta {
                    meta.size = stats_size;
                    meta.location = (stats_location, SegmentStatistics::VERSION);
                }
            }

            segment.format_version = SegmentInfo::VERSION;
            segment
                .write_meta_through_cache(operator, &segment_location)
                .await?;
            new_segment_locations.push((segment_location, SegmentInfo::VERSION));
        }
    }

    let mut new_snapshot = TableSnapshot::try_from_previous(
        base_snapshot.clone(),
        fuse_table.cluster_key_info(),
        Some(fuse_table.get_table_info().ident.seq),
        table_meta_timestamps,
    )?;
    new_snapshot.schema = new_schema.as_ref().clone();
    new_snapshot.segments = new_segment_locations;
    for rewrite in column_rewrites {
        new_snapshot
            .summary
            .widen_decimal_column(rewrite.column_id, rewrite.size)?;
    }
    widen_cluster_statistics(&mut new_snapshot.summary.cluster_stats, cluster_rewrites)?;

    if let Some(table_stats) = fuse_table
        .read_table_snapshot_statistics(Some(&base_snapshot))
        .await?
        .filter(|stats| stats.is_fresh_for(&base_snapshot))
    {
        let mut table_stats = table_stats.as_ref().clone();
        for rewrite in column_rewrites {
            table_stats.widen_decimal_column(rewrite.column_id, rewrite.size)?;
        }
        // This sidecar was fresh for the base snapshot and describes unchanged data, so retag
        // its Decimal Top-N values and keep it fresh for the new metadata-only snapshot. A stale
        // sidecar is inherited unchanged above and must not participate in this rewrite.
        table_stats.snapshot_id = base_snapshot.snapshot_id;
        let location = fuse_table
            .meta_location_generator()
            .snapshot_statistics_location_from_uuid(
                &SnapshotId::now_v7(),
                table_stats.format_version(),
            )?;
        table_stats.write_meta(operator, &location).await?;
        new_snapshot.table_statistics_location = Some(location);
    }

    let new_snapshot_location = fuse_table
        .meta_location_generator()
        .gen_snapshot_location(&new_snapshot.snapshot_id, TableSnapshot::VERSION)?;
    new_snapshot
        .write_meta(operator, &new_snapshot_location)
        .await?;

    new_table_meta.schema = new_schema;
    new_table_meta.options.insert(
        OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
        new_snapshot_location.clone(),
    );
    new_table_meta.updated_on = Utc::now();
    update_table_meta(fuse_table, &new_table_meta, catalog, ctx.get_tenant()).await?;
    FuseTable::write_last_snapshot_hint(
        ctx.as_ref(),
        operator,
        fuse_table.meta_location_generator(),
        &new_snapshot_location,
        &new_table_meta,
    )
    .await;
    Ok(())
}

fn decimal_parquet_physical_type(data_type: &TableDataType) -> Result<(parquet::basic::Type, i32)> {
    let schema = TableSchema::new(vec![TableField::new("decimal", data_type.clone())]);
    let arrow_schema = ArrowSchema::from(&schema);
    let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema)?;
    let column = parquet_schema.column(0);
    Ok((column.physical_type(), column.type_length()))
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
    let new_table = FuseTable::create_and_refresh_table_info(
        table_info,
        ctx.get_settings().get_s3_storage_class()?,
    )?;

    // 4. build DistributedInsertSelect plan
    let mut insert_plan = PhysicalPlan::new(DistributedInsertSelect {
        input: select_plan,
        table_info: new_table.get_table_info().clone(),
        select_schema,
        select_column_bindings,
        insert_schema: Arc::new(new_schema.into()),
        cast_needed: true,
        input_prepared: false,
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
