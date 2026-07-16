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

use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_meta_app::schema::MVDefinition;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use log::info;

use crate::BindContext;
use crate::MaterializedViewAggregateDesc;
use crate::MaterializedViewScanInfo;
use crate::Metadata;
use crate::ScalarExpr;
use crate::Visibility;
use crate::binder::Binder;
use crate::binder::ScalarBinder;
use crate::binder::ddl::materialized_view::find_materialized_view_aggregate;
use crate::optimizer::ir::SExpr;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;

impl Binder {
    /// Whether the MV storage is still consistent with its source table snapshot.
    ///
    /// Uses the same option pair as the synchronous multi-table insert path:
    /// `materialized_view_source_snapshot_location` vs source `snapshot_loc`.
    fn is_materialized_view_fresh(
        mv_table: &dyn Table,
        source_snapshot_location: Option<&String>,
    ) -> bool {
        let mv_source_snapshot = mv_table
            .options()
            .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION);
        mv_source_snapshot == source_snapshot_location
    }

    // Stale MV read: bind the persisted logical definition against the live source table.
    // TODO: Support hybrid read in storage layer.
    fn bind_materialized_view_fallback(
        &mut self,
        bind_context: &mut BindContext,
        mv_definition: &MVDefinition,
        database: &str,
        table_name: &str,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let tokens = tokenize_sql(&mv_definition.original_query)?;
        let (stmt, _) = parse_sql(&tokens, self.dialect)?;
        let Statement::Query(query) = &stmt else {
            return Err(ErrorCode::Internal(
                "Invalid materialized view logical query",
            ));
        };

        // Bind with the current binder so the source table enters this query's metadata.
        let mut definition_context = BindContext::with_parent(bind_context.clone())?;
        let (s_expr, mut logical_context) = self.bind_query(&mut definition_context, query)?;
        if logical_context.columns.len() != mv_definition.logical_schema.num_fields() {
            return Err(ErrorCode::Internal(format!(
                "materialized view logical query has {} columns, expected {}",
                logical_context.columns.len(),
                mv_definition.logical_schema.num_fields()
            )));
        }
        for (column, field) in logical_context
            .columns
            .iter_mut()
            .zip(mv_definition.logical_schema.fields())
        {
            let expected_type = DataType::from(field.data_type());
            if column.data_type.as_ref() != &expected_type {
                return Err(ErrorCode::Internal(format!(
                    "materialized view logical column '{}' expects {}, query produces {}",
                    field.name(),
                    expected_type,
                    column.data_type
                )));
            }
            column.column_name = field.name().clone();
        }
        if let Some(alias) = alias {
            logical_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        } else {
            for column in logical_context.columns.iter_mut() {
                column.database_name = Some(database.to_string());
                column.table_name = Some(table_name.to_string());
            }
        }
        Ok((s_expr, logical_context))
    }

    fn build_materialized_view_scan_info(
        &self,
        s_expr: &SExpr,
        physical_schema: &databend_common_expression::TableSchema,
    ) -> Result<Option<MaterializedViewScanInfo>> {
        let Some(aggregate) = find_materialized_view_aggregate(s_expr) else {
            return Ok(None);
        };
        if aggregate.aggregate_functions.is_empty() && aggregate.group_items.is_empty() {
            return Ok(None);
        }

        let aggregate_functions = aggregate
            .aggregate_functions
            .iter()
            .map(|item| {
                let ScalarExpr::AggregateFunction(function) = &item.scalar else {
                    return Err(ErrorCode::Unimplemented(
                        "materialized view read only supports built-in aggregate functions",
                    ));
                };
                let argument_types = function
                    .args
                    .iter()
                    .map(ScalarExpr::data_type)
                    .collect::<Result<Vec<_>>>()?;
                Ok(MaterializedViewAggregateDesc {
                    name: function
                        .func_name
                        .strip_suffix("_state")
                        .unwrap_or(&function.func_name)
                        .to_string(),
                    params: function.params.clone(),
                    argument_types,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let num_aggregates = aggregate_functions.len();
        let num_groups = aggregate.group_items.len();
        if physical_schema.num_fields() != num_aggregates + num_groups {
            return Err(ErrorCode::Internal(format!(
                "materialized view state schema has {} columns, expected {} aggregate states and {} group columns",
                physical_schema.num_fields(),
                num_aggregates,
                num_groups
            )));
        }

        let physical_data_types = physical_schema
            .fields()
            .iter()
            .map(|field| DataType::from(field.data_type()))
            .collect::<Vec<_>>();
        let group_data_types = physical_data_types[num_aggregates..].to_vec();

        let mut final_data_types = aggregate_functions
            .iter()
            .enumerate()
            .map(|(offset, aggregate)| {
                let result_type = AggregateFunctionFactory::instance()
                    .get(
                        &aggregate.name,
                        aggregate.params.clone(),
                        aggregate.argument_types.clone(),
                        vec![],
                    )?
                    .return_type()?;
                if physical_data_types[offset].is_nullable() && !result_type.is_nullable() {
                    Ok(result_type.wrap_nullable())
                } else {
                    Ok(result_type)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        final_data_types.extend(group_data_types.iter().cloned());

        Ok(Some(MaterializedViewScanInfo {
            aggregate_functions,
            physical_data_types,
            final_data_types,
            group_data_types,
        }))
    }

    fn build_materialized_view_final_projection(
        &mut self,
        physical_context: &BindContext,
        logical_schema: &databend_common_expression::TableSchema,
        child: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        let physical_columns = physical_context
            .columns
            .iter()
            .filter(|column| column.visibility == Visibility::Visible)
            .cloned()
            .collect::<Vec<_>>();
        let mut expression_context = BindContext::new();
        expression_context.columns = physical_columns;

        let mut items = Vec::with_capacity(logical_schema.num_fields());
        let mut output_columns = Vec::with_capacity(logical_schema.num_fields());
        for logical_field in logical_schema.fields() {
            // For MVDefinition.schema only, default_expr stores the canonical finalized
            // projection expression rather than a table-column default value.
            let final_expr = logical_field.default_expr().ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "materialized view logical column '{}' has no final expression; drop and recreate it",
                    logical_field.name()
                ))
            })?;
            let tokens = tokenize_sql(final_expr)?;
            let ast = parse_expr(&tokens, self.dialect)?;
            let (scalar, scalar_type) = {
                let mut scalar_binder = ScalarBinder::new(
                    &mut expression_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                scalar_binder.forbid_udf();
                scalar_binder.bind(&ast)?
            };
            let logical_type = DataType::from(logical_field.data_type());
            if scalar_type != logical_type {
                return Err(ErrorCode::Internal(format!(
                    "materialized view logical column '{}' expects {}, final expression produces {}; drop and recreate it",
                    logical_field.name(),
                    logical_type,
                    scalar_type
                )));
            }
            let output =
                self.create_derived_column_binding(logical_field.name().to_string(), logical_type);
            items.push(ScalarItem {
                scalar,
                index: output.index,
            });
            output_columns.push(output);
        }

        let projection =
            SExpr::create_unary(Arc::new(EvalScalar { items }.into()), Arc::new(child));
        let mut output_context = BindContext::new();
        output_context.parent = physical_context.parent.clone();
        output_context
            .cte_context
            .set_cte_context(physical_context.cte_context.clone());
        output_context.columns = output_columns;
        Ok((projection, output_context))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn bind_materialized_view(
        &mut self,
        bind_context: &mut BindContext,
        catalog_name: &str,
        database: &str,
        table_name: &str,
        table_name_alias: Option<String>,
        table_meta: Arc<dyn Table>,
        alias: &Option<TableAlias>,
        sample: &Option<SampleConfig>,
        cte_suffix_name: Option<String>,
    ) -> Result<(SExpr, BindContext)> {
        let tenant = self.ctx.get_tenant();
        let (mv_definition, source_snapshot_location) =
            databend_common_base::runtime::block_on(async {
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                let mv_definition = catalog
                    .get_mv_definition(&tenant, table_meta.get_id())
                    .await?
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "materialized view {} has no definition",
                            table_meta.name()
                        ))
                    })?;
                let source_table_id = table_meta
                    .get_table_info()
                    .meta
                    .materialized_view_source_table_id()
                    .map_err(ErrorCode::from)?;
                let source_meta = catalog
                    .get_table_meta_by_id(source_table_id)
                    .await?
                    .ok_or_else(|| {
                        ErrorCode::UnknownTable(format!(
                            "materialized view {} source table id {} not found",
                            table_meta.name(),
                            source_table_id
                        ))
                    })?;
                let source_snapshot_location = source_meta
                    .data
                    .options
                    .get(OPT_KEY_SNAPSHOT_LOCATION)
                    .cloned();
                Ok::<_, ErrorCode>((mv_definition, source_snapshot_location))
            })?;

        if !Self::is_materialized_view_fresh(table_meta.as_ref(), source_snapshot_location.as_ref())
        {
            info!(
                "materialized view {} is stale (source_snapshot={:?}, base_snapshot={:?}); fallback to live compute",
                table_meta.name(),
                table_meta
                    .options()
                    .get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION),
                source_snapshot_location
            );
            return self.bind_materialized_view_fallback(
                bind_context,
                &mv_definition.data,
                database,
                table_name,
                alias,
            );
        }

        let tokens = tokenize_sql(&mv_definition.data.query)?;
        let (stmt, _) = parse_sql(&tokens, self.dialect)?;
        let Statement::Query(query) = &stmt else {
            return Err(ErrorCode::Internal("Invalid materialized view query"));
        };
        let mut definition_binder = Binder::new(
            self.ctx.clone(),
            self.catalogs.clone(),
            self.name_resolution_ctx.clone(),
            Metadata::default_ref(),
        )
        .with_subquery_executor(self.subquery_executor.clone());
        let mut definition_context = BindContext::new();
        let (definition_expr, _) = definition_binder.bind_query(&mut definition_context, query)?;
        let storage_scan_info = definition_binder
            .build_materialized_view_scan_info(&definition_expr, table_meta.schema().as_ref())?;

        let table_index = self.metadata.write().add_table(
            catalog_name.to_string(),
            database.to_string(),
            table_meta,
            None,
            table_name_alias,
            !bind_context.binding_views.is_empty(),
            bind_context.planning_agg_index,
            false,
            cte_suffix_name,
        );
        if let Some(scan_info) = storage_scan_info {
            let mut metadata = self.metadata.write();
            metadata
                .set_materialized_view_column_types(table_index, &scan_info.final_data_types)?;
            metadata.set_materialized_view_scan(table_index, scan_info);
        }

        let (s_expr, physical_context) =
            self.bind_base_table(bind_context, database, table_index, None, sample, false)?;
        let (s_expr, mut logical_context) = self.build_materialized_view_final_projection(
            &physical_context,
            &mv_definition.data.logical_schema,
            s_expr,
        )?;
        if let Some(alias) = alias {
            logical_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        } else {
            for column in logical_context.columns.iter_mut() {
                column.database_name = Some(database.to_string());
                column.table_name = Some(table_name.to_string());
            }
        }
        Ok((s_expr, logical_context))
    }
}
