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

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::MVDefinition;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use log::info;

use crate::BindContext;
use crate::Metadata;
use crate::Visibility;
use crate::binder::Binder;
use crate::binder::ScalarBinder;
use crate::binder::ddl::materialized_view::find_materialized_view_aggregate;
use crate::optimizer::ir::SExpr;
use crate::parse_materialized_view_query;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::validate_materialized_view_source;

impl Binder {
    /// Whether the MV storage checkpoint matches the current source data endpoint.
    fn is_materialized_view_fresh(
        mv_table: &dyn Table,
        source_snapshot_location: Option<&String>,
    ) -> bool {
        let options = mv_table.options();
        // The sequence option distinguishes an established empty checkpoint from CREATE's
        // unconsumed state. Its value is a CHANGE_TRACKING offset, not part of read freshness:
        // metadata-only source changes may advance the table sequence without changing data.
        options.contains_key(OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_SEQ)
            && options.get(OPT_KEY_MATERIALIZED_VIEW_SOURCE_SNAPSHOT_LOCATION)
                == source_snapshot_location
    }

    // Stale MV read: bind the persisted logical definition against the live source table.
    // TODO: Support hybrid read in storage layer.
    fn bind_materialized_view_fallback(
        &mut self,
        bind_context: &mut BindContext,
        mv_definition: &MVDefinition,
        source_database: &str,
        source_table_name: &str,
        database: &str,
        table_name: &str,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let mut query = parse_materialized_view_query(
            &mv_definition.original_query,
            "invalid materialized view logical query",
        )?;
        Self::rewrite_materialized_view_source(&mut query, source_database, source_table_name)?;

        // Bind with the current binder so the source table enters this query's metadata.
        let mut definition_context = BindContext::with_parent(bind_context.clone())?;
        let (s_expr, mut logical_context) = self.bind_query(&mut definition_context, &query)?;
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

    fn rewrite_materialized_view_source(
        query: &mut Query,
        source_database: &str,
        source_table_name: &str,
    ) -> Result<()> {
        let SetExpr::Select(select) = &mut query.body else {
            return Err(ErrorCode::Internal(
                "materialized view query must be a SELECT",
            ));
        };
        let [TableReference::Table { table, .. }] = select.from.as_mut_slice() else {
            return Err(ErrorCode::Internal(
                "materialized view query must have exactly one table source",
            ));
        };
        table.database = Some(Identifier::from_name(None, source_database));
        table.table = Identifier::from_name(None, source_table_name);
        Ok(())
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
            // For MVDefinition.logical_schema only, default_expr stores the canonical finalized
            // projection expression rather than a table-column default value. A missing expression
            // is the compact representation of an identity projection from the same-named physical
            // column.
            let (scalar, scalar_type) = if let Some(final_expr) = logical_field.default_expr() {
                let tokens = tokenize_sql(final_expr)?;
                let ast = parse_expr(&tokens, Dialect::PostgreSQL)?;
                let mut scalar_binder = ScalarBinder::new(
                    &mut expression_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                scalar_binder.forbid_udf();
                scalar_binder.bind(&ast)?
            } else {
                let column = expression_context
                    .columns
                    .iter()
                    .find(|column| column.column_name == *logical_field.name())
                    .cloned()
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "materialized view physical column '{}' does not exist; drop and recreate it",
                            logical_field.name()
                        ))
                    })?;
                let scalar_type = *column.data_type.clone();
                (
                    ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
                    scalar_type,
                )
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
        let (
            mv_definition,
            source_table_id,
            source_snapshot_location,
            source_database,
            source_table_name,
        ) = databend_common_base::runtime::block_on(async {
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
            let source_database_id = source_meta
                .data
                .options
                .get(OPT_KEY_DATABASE_ID)
                .ok_or_else(|| ErrorCode::Internal("source table database id is missing"))?
                .parse::<u64>()?;
            let source_database = catalog.get_db_name_by_id(source_database_id).await?;
            let source_table_name = catalog
                .get_table_name_by_id(source_table_id)
                .await?
                .ok_or_else(|| {
                    ErrorCode::UnknownTable(format!(
                        "materialized view {} source table id {} not found",
                        table_meta.name(),
                        source_table_id
                    ))
                })?;
            Ok::<_, ErrorCode>((
                mv_definition,
                source_table_id,
                source_snapshot_location,
                source_database,
                source_table_name,
            ))
        })?;

        // Aggregate MV storage contains serialized states. Reading and merging those states across
        // cluster nodes needs an explicit distributed plan boundary, which is deferred to a follow-up
        // change. Until then, aggregate MVs always execute their persisted logical definition against
        // the source table, even when their storage checkpoint is fresh.
        let mut query = parse_materialized_view_query(
            &mv_definition.data.query,
            "invalid materialized view physical query",
        )?;
        let definition_metadata = Metadata::default_ref();
        Self::rewrite_materialized_view_source(&mut query, &source_database, &source_table_name)?;
        let mut definition_binder = Binder::new(
            self.ctx.clone(),
            self.catalogs.clone(),
            self.name_resolution_ctx.clone(),
            definition_metadata.clone(),
        )
        .with_subquery_executor(self.subquery_executor.clone());
        let mut definition_context = BindContext::new();
        let (definition_expr, _) = definition_binder.bind_query(&mut definition_context, &query)?;
        validate_materialized_view_source(
            &definition_metadata,
            source_table_id,
            table_meta.name(),
        )?;
        if find_materialized_view_aggregate(&definition_expr)
            .is_some_and(|aggregate| !aggregate.aggregate_functions.is_empty())
        {
            return self.bind_materialized_view_fallback(
                bind_context,
                &mv_definition.data,
                &source_database,
                &source_table_name,
                database,
                table_name,
                alias,
            );
        }

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
                &source_database,
                &source_table_name,
                database,
                table_name,
                alias,
            );
        }

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
