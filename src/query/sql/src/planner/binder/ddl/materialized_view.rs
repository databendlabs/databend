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

use databend_common_ast::ast::CreateMaterializedViewStmt;
use databend_common_ast::ast::DropMaterializedViewStmt;
use databend_common_ast::ast::RefreshMaterializedViewStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowMaterializedViewsStmt;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::visit::WalkMut;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use log::debug;

use crate::BindContext;
use crate::MetadataRef;
use crate::SelectBuilder;
use crate::binder::Binder;
use crate::optimizer::ir::SExpr;
use crate::planner::SUPPORTED_AGGREGATING_INDEX_FUNCTIONS;
use crate::planner::semantic::MaterializedViewChecker;
use crate::planner::semantic::MaterializedViewRewriter;
use crate::planner::semantic::ViewRewriter;
use crate::planner::semantic::normalize_identifier;
use crate::plans::Aggregate;
use crate::plans::CreateMaterializedViewPlan;
use crate::plans::DropMaterializedViewPlan;
use crate::plans::Plan;
use crate::plans::RefreshMaterializedViewPlan;
use crate::plans::RelOperator;
use crate::plans::RewriteKind;
use crate::plans::ScalarExpr;

/// Walk a unary plan chain and return the first Aggregate node, if any.
pub(crate) fn find_materialized_view_aggregate(s_expr: &SExpr) -> Option<&Aggregate> {
    let mut current = s_expr;
    loop {
        if let RelOperator::Aggregate(aggregate) = current.plan() {
            return Some(aggregate);
        }
        let Ok(child) = current.child(0) else {
            return None;
        };
        current = child;
    }
}

/// Table schemas reject bare Null columns; normalize them the same way CREATE TABLE does.
fn normalize_null_fields(schema: TableSchemaRef) -> TableSchemaRef {
    let mut fields = schema.fields().clone();
    let mut changed = false;
    for field in fields.iter_mut() {
        if field.data_type == TableDataType::Null {
            field.data_type = TableDataType::String.wrap_nullable();
            changed = true;
        }
    }
    if changed {
        TableSchemaRefExt::create(fields)
    } else {
        schema
    }
}

impl Binder {
    /// Build the physical schema from rewriter names and types inferred by binding.
    /// Aggregate outputs are persisted as serialized states, while group keys and
    /// non-aggregate outputs use their bound result types.
    fn materialized_view_physical_schema(
        s_expr: &SExpr,
        bind_context: &BindContext,
        metadata: MetadataRef,
        physical_names: &[String],
    ) -> Result<TableSchemaRef> {
        let data_types = if let Some(aggregate) = find_materialized_view_aggregate(s_expr) {
            let mut data_types = Vec::with_capacity(
                aggregate.aggregate_functions.len() + aggregate.group_items.len(),
            );
            for item in &aggregate.aggregate_functions {
                let ScalarExpr::AggregateFunction(function) = &item.scalar else {
                    return Err(ErrorCode::Unimplemented(
                        "materialized view only supports built-in aggregate functions",
                    ));
                };
                let agg_name = function
                    .func_name
                    .strip_suffix("_state")
                    .unwrap_or(&function.func_name);
                let argument_types = function
                    .args
                    .iter()
                    .map(ScalarExpr::data_type)
                    .collect::<Result<Vec<_>>>()?;
                let state_type = AggregateFunctionFactory::instance()
                    .get(agg_name, function.params.clone(), argument_types, vec![])?
                    .serialize_data_type();
                data_types.push(infer_schema_type(&state_type)?);
            }
            for item in &aggregate.group_items {
                data_types.push(infer_schema_type(&item.scalar.data_type()?)?);
            }
            data_types
        } else {
            bind_context
                .output_table_schema(metadata)?
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect()
        };

        if data_types.len() != physical_names.len() {
            return Err(ErrorCode::Internal(format!(
                "materialized view rewriter produced {} physical names, bound plan has {} fields",
                physical_names.len(),
                data_types.len()
            )));
        }
        Ok(TableSchemaRefExt::create(
            physical_names
                .iter()
                .zip(data_types)
                .map(|(name, data_type)| TableField::new(name, data_type))
                .collect(),
        ))
    }

    /// Build the logical schema from names and expressions recorded by the rewriter.
    /// Types come from the bound original query.
    fn materialized_view_logical_schema_from_rewriter(
        logical_context: &BindContext,
        rewriter: &MaterializedViewRewriter,
        metadata: MetadataRef,
    ) -> Result<TableSchemaRef> {
        let schema = logical_context.output_table_schema(metadata)?;
        let names = rewriter.logical_names();
        let define_exprs = rewriter.logical_define_exprs();
        if schema.num_fields() != names.len() || schema.num_fields() != define_exprs.len() {
            return Err(ErrorCode::Internal(format!(
                "materialized view has {} logical outputs, rewriter recorded {} names and {} expressions",
                schema.num_fields(),
                names.len(),
                define_exprs.len()
            )));
        }
        let fields = schema
            .fields()
            .iter()
            .zip(names)
            .zip(define_exprs)
            .map(|((field, name), define_expr)| {
                TableField::new(name, field.data_type().clone())
                    .with_default_expr(Some(define_expr.clone()))
            })
            .collect();
        Ok(TableSchemaRefExt::create(fields))
    }

    /// Resolve the single base table referenced by the defining query.
    fn materialized_view_source_table(metadata: &MetadataRef) -> Result<(Arc<dyn Table>, String)> {
        let metadata = metadata.read();
        if metadata.tables().len() != 1 {
            return Err(ErrorCode::SemanticError(
                "Materialized view requires exactly one base table source".to_string(),
            ));
        }
        let table = &metadata.tables()[0];
        Ok((table.table(), table.database().to_string()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_materialized_view(
        &mut self,
        stmt: &CreateMaterializedViewStmt,
    ) -> Result<Plan> {
        let CreateMaterializedViewStmt {
            create_option,
            catalog,
            database,
            view,
            columns,
            query,
        } = stmt;

        // Reject unsupported syntax before binding. The rest of CREATE can then rely on a single
        // plain table source and on the SELECT/FROM/WHERE/GROUP BY shape understood by refresh.
        let checker = MaterializedViewChecker::check_query(query);
        if !checker.is_supported() {
            return Err(ErrorCode::SemanticError(format!(
                "Materialized View only support simple query, like: \
                    `SELECT ... FROM ... WHERE ... GROUP BY ...`, \
                     and these aggregate funcs: {}, \
                     non-deterministic functions are not support like: NOW()",
                SUPPORTED_AGGREGATING_INDEX_FUNCTIONS.join(",")
            )));
        }

        // 1. Resolve the target database and bind the user-facing definition. The logical bind is
        // authoritative for finalized output types and for the concrete source table identity.
        let tenant = self.ctx.get_tenant();
        let (catalog_name, database_name, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let target_catalog = self.ctx.get_catalog(&catalog_name).await?;
        let target_database = target_catalog.get_database(&tenant, &database_name).await?;
        let target_database_id = target_database.get_db_info().database_id.db_id;

        let original_query_plan = self.as_query_plan(query).await?;
        let Plan::Query {
            metadata: original_metadata,
            bind_context: original_bind_context,
            ..
        } = &original_query_plan
        else {
            return Err(ErrorCode::Internal(
                "materialized view AS clause must produce a Query plan",
            ));
        };
        let (source_table, source_database) =
            Self::materialized_view_source_table(original_metadata)?;
        if source_table.engine() != "FUSE" {
            return Err(ErrorCode::SemanticError(format!(
                "Materialized view source table '{}.{}' must use FUSE engine, but uses {}",
                source_database,
                source_table.name(),
                source_table.engine()
            )));
        }
        if !source_table.change_tracking_enabled() {
            return Err(ErrorCode::SemanticError(format!(
                "Materialized view source table '{}.{}' must have CHANGE_TRACKING enabled",
                source_database,
                source_table.name()
            )));
        }
        let source_table_id = source_table.get_id();

        // Qualify the logical definition once so stale fallback is independent of the session's
        // current database. The physical definition starts from that same canonical source SQL.
        let mut original_query = query.as_ref().clone();
        original_query.walk_mut(&mut ViewRewriter {
            current_database: source_database.clone(),
        })?;

        // 2. Rewrite the qualified definition into storage form, recording logical-to-physical
        // output mapping in the same pass.
        let mut physical_query = original_query.clone();
        let specified_columns = columns
            .iter()
            .map(|column| normalize_identifier(column, &self.name_resolution_ctx).name)
            .collect();
        let mut physical_rewriter = MaterializedViewRewriter::new(
            checker.is_aggregating(),
            &source_database,
            specified_columns,
        );
        physical_rewriter.rewrite_query(&mut physical_query)?;

        // 3. Bind storage SQL independently: aggregate outputs now have physical serialized-state
        // semantics, so this plan cannot be shared with the logical definition bind above.
        let mut physical_binder = Binder::new(
            self.ctx.clone(),
            self.catalogs.clone(),
            self.name_resolution_ctx.clone(),
            crate::Metadata::default_ref(),
        )
        .with_subquery_executor(self.subquery_executor.clone());
        let physical_query_plan = physical_binder.as_query_plan(&physical_query).await?;
        let Plan::Query {
            s_expr: storage_expr,
            metadata: physical_metadata,
            bind_context: physical_bind_context,
            ..
        } = &physical_query_plan
        else {
            return Err(ErrorCode::Internal(
                "materialized view storage query must produce a Query plan",
            ));
        };
        let physical_schema = normalize_null_fields(Self::materialized_view_physical_schema(
            storage_expr,
            physical_bind_context,
            physical_metadata.clone(),
            physical_rewriter.physical_names(),
        )?);
        Self::validate_create_table_schema(&physical_schema)?;

        // 4. Logical schema keeps the original finalized types and the rewriter's final projection.
        let logical_schema =
            normalize_null_fields(Self::materialized_view_logical_schema_from_rewriter(
                original_bind_context,
                &physical_rewriter,
                original_metadata.clone(),
            )?);
        Self::validate_create_table_schema(&logical_schema)?;

        // 5. Assemble CreateMaterializedViewPlan: the physical schema is used by
        //    Fuse storage; MVDefinition holds the original query, rewritten storage
        //    query, and logical schema (with final projection expressions).
        let mut options = BTreeMap::new();
        options.insert(
            OPT_KEY_DATABASE_ID.to_owned(),
            target_database_id.to_string(),
        );
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_owned(),
            source_table_id.to_string(),
        );

        let mv_definition = MVDefinition {
            original_query: original_query.to_string(),
            query: physical_query.to_string(),
            logical_schema: logical_schema.as_ref().clone(),
            sync_creation: false,
        };

        let plan = CreateMaterializedViewPlan {
            create_option: create_option.clone().into(),
            tenant,
            catalog: catalog_name,
            database: database_name,
            view_name,
            schema: physical_schema,
            options,
            mv_definition,
        };

        Ok(Plan::CreateMaterializedView(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_materialized_view(
        &mut self,
        stmt: &DropMaterializedViewStmt,
    ) -> Result<Plan> {
        let DropMaterializedViewStmt {
            if_exists,
            catalog,
            database,
            view,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let plan = DropMaterializedViewPlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            view_name,
        };
        Ok(Plan::DropMaterializedView(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_materialized_view(
        &mut self,
        stmt: &RefreshMaterializedViewStmt,
    ) -> Result<Plan> {
        let RefreshMaterializedViewStmt {
            catalog,
            database,
            view,
        } = stmt;
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);

        Ok(Plan::RefreshMaterializedView(Box::new(
            RefreshMaterializedViewPlan {
                tenant: self.ctx.get_tenant(),
                catalog,
                database,
                view_name,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_materialized_views(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowMaterializedViewsStmt,
    ) -> Result<Plan> {
        let ShowMaterializedViewsStmt {
            catalog,
            database,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => normalize_identifier(ident, &self.name_resolution_ctx).name,
        };

        let database_name = self.check_database_exist(catalog, database).await?;

        let mut select_builder = SelectBuilder::from(&format!(
            "{}.system.tables",
            QuotedIdent(catalog_name.to_lowercase(), '`')
        ));
        select_builder
            .with_column("name AS Name")
            .with_column("database AS Database")
            .with_column("engine AS Engine")
            .with_column("created_on AS \"Created On\"");

        select_builder.with_filter(format!("database = {}", QuotedString(&database_name, '\'')));
        select_builder.with_filter(format!("catalog = {}", QuotedString(&catalog_name, '\'')));
        select_builder.with_filter("table_type = 'MATERIALIZED VIEW'".to_string());

        select_builder
            .with_order_by("database")
            .with_order_by("name");

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE {}", QuotedString(pattern, '\'')));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show materialized views rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog_name, database_name),
        )
        .await
    }
}
