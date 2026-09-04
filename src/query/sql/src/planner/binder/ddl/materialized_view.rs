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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::AlterMaterializedViewStmt;
use databend_common_ast::ast::AlterTableAction;
use databend_common_ast::ast::ClusterType as AstClusterType;
use databend_common_ast::ast::CreateMaterializedViewStmt;
use databend_common_ast::ast::DropMaterializedViewStmt;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::RefreshMaterializedViewStmt;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::ShowCreateMaterializedViewStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowMaterializedViewsStmt;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::quote::QuotedIdent;
use databend_common_ast::ast::quote::QuotedString;
use databend_common_ast::visit::WalkMut;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_enterprise_materialized_view::get_materialized_view_handler;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::OPT_KEY_AGGRESSIVE_RECLUSTER;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_COMMENT;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use databend_storages_common_table_meta::table::OPT_KEY_WRITE_DISTRIBUTION_MODE;
use databend_storages_common_table_meta::table::TableCompression;
use databend_storages_common_table_meta::table::WriteDistributionMode;
use databend_storages_common_table_meta::table::is_fuse_engine;
use log::debug;

use crate::BindContext;
use crate::MaterializedViewCandidate;
use crate::MaterializedViewCandidateReadMode;
use crate::MetadataRef;
use crate::SelectBuilder;
use crate::TableEntry;
use crate::binder::Binder;
use crate::binder::bind_table_reference::MaterializedViewReadMode;
use crate::optimizer::ir::SExpr;
use crate::parse_materialized_view_query;
use crate::planner::semantic::MaterializedViewChecker;
use crate::planner::semantic::MaterializedViewRewriter;
use crate::planner::semantic::ViewRewriter;
use crate::planner::semantic::normalize_identifier;
use crate::plans::Aggregate;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::CreateMaterializedViewPlan;
use crate::plans::CreateTablePlan;
use crate::plans::DropMaterializedViewPlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::MaintenanceTarget;
use crate::plans::ModifyTableCommentPlan;
use crate::plans::Plan;
use crate::plans::ReclusterPlan;
use crate::plans::RefreshMaterializedViewPlan;
use crate::plans::RelOperator;
use crate::plans::RewriteKind;
use crate::plans::ScalarExpr;
use crate::plans::SetOptionsPlan;
use crate::plans::ShowCreateMaterializedViewPlan;
use crate::plans::UnsetOptionsPlan;

fn is_supported_materialized_view_source(table: &dyn Table) -> bool {
    !table.is_temp()
        && !table.is_stream()
        && !table.is_read_only()
        && is_fuse_engine(table.engine())
}

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
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_query_materialized_views(
        &mut self,
        bind_context: &mut BindContext,
        plan: &Plan,
    ) -> Result<()> {
        let metadata = match plan {
            Plan::Query { metadata, .. } => Some(metadata),
            Plan::Explain { kind, plan, .. }
                if matches!(kind, ExplainKind::Plan) && matches!(**plan, Plan::Query { .. }) =>
            {
                match plan.as_ref() {
                    Plan::Query { metadata, .. } => Some(metadata),
                    _ => unreachable!(),
                }
            }
            _ => None,
        };
        if let Some(metadata) = metadata {
            self.do_bind_query_materialized_views(bind_context, metadata)
                .await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn do_bind_query_materialized_views(
        &mut self,
        bind_context: &mut BindContext,
        metadata: &MetadataRef,
    ) -> Result<()> {
        if bind_context.planning_agg_index
            || bind_context.planning_materialized_view_rewrite
            || !self.ctx.get_can_scan_from_agg_index()
            || !self.enable_materialized_view_rewrite
        {
            return Ok(());
        }

        let tenant = self.ctx.get_tenant();
        let source_entries = metadata.read().tables().to_vec();
        for source_entry in source_entries {
            if source_entry.is_source_of_view()
                || source_entry.is_source_of_index()
                || source_entry.is_source_of_stage()
            {
                continue;
            }
            let source_table = source_entry.table();
            if !is_fuse_engine(source_table.engine()) {
                continue;
            }

            let catalog = self.ctx.get_catalog(source_entry.catalog()).await?;
            let source_table_id = source_table.get_id();
            let snapshot = catalog
                .get_mv_source_binding_snapshot(&tenant, source_table_id)
                .await?;
            if snapshot.materialized_views.is_empty() {
                continue;
            }

            let source_info = source_table.get_table_info();
            let source_table_seq = source_info.ident.seq;
            let source_snapshot_location = source_info
                .meta
                .options
                .get(OPT_KEY_SNAPSHOT_LOCATION)
                .cloned();
            let mut candidates = Vec::with_capacity(snapshot.materialized_views.len());
            for mv in snapshot.materialized_views {
                if mv
                    .table_meta
                    .data
                    .materialized_view_source_table_id()
                    .map_err(ErrorCode::from)?
                    != source_table_id
                {
                    continue;
                }
                let database_id = mv
                    .table_meta
                    .data
                    .options
                    .get(OPT_KEY_DATABASE_ID)
                    .ok_or_else(|| ErrorCode::Internal("materialized view database id is missing"))?
                    .parse::<u64>()?;
                let mv_database = catalog.get_db_name_by_id(database_id).await?;
                let Some(mv_name) = catalog.get_table_name_by_id(mv.mv_id).await? else {
                    continue;
                };
                let current_mv = catalog.get_table(&tenant, &mv_database, &mv_name).await?;
                let mv_table =
                    catalog.get_table_by_info(&databend_common_meta_app::schema::TableInfo {
                        ident: databend_common_meta_app::schema::TableIdent::new(
                            mv.mv_id,
                            mv.table_meta.seq,
                        ),
                        meta: mv.table_meta.data.clone(),
                        ..current_mv.get_table_info().clone()
                    })?;

                let logical_query = parse_materialized_view_query(
                    &mv.definition.data.original_query,
                    "invalid materialized view logical query",
                )?;
                let mut candidate_context = BindContext::with_parent(bind_context.clone())?;
                candidate_context.planning_materialized_view_rewrite = true;
                let (definition, definition_context) =
                    self.bind_query(&mut candidate_context, &logical_query)?;

                let mut read_context = BindContext::with_parent(bind_context.clone())?;
                read_context.planning_materialized_view_rewrite = true;
                let (read_plan, read_context, read_mode) = self.bind_materialized_view_with_mode(
                    &mut read_context,
                    source_entry.catalog(),
                    &mv_database,
                    &mv_name,
                    None,
                    mv_table,
                    &None,
                    &None,
                    None,
                    Some((source_table.clone(), source_entry.database().to_string())),
                    false,
                )?;
                let read_mode = match read_mode {
                    MaterializedViewReadMode::Fresh => MaterializedViewCandidateReadMode::Fresh,
                    MaterializedViewReadMode::Hybrid => MaterializedViewCandidateReadMode::Hybrid,
                    MaterializedViewReadMode::LiveFallback => continue,
                };
                candidates.push(MaterializedViewCandidate {
                    source_table_id,
                    source_table_index: source_entry.index(),
                    mv_table_id: mv.mv_id,
                    definition,
                    read_plan,
                    read_mode,
                    logical_sql: mv.definition.data.original_query.clone(),
                    mv_table_seq: mv.table_meta.seq,
                    mv_snapshot_location: mv
                        .table_meta
                        .data
                        .options
                        .get(OPT_KEY_SNAPSHOT_LOCATION)
                        .cloned(),
                    source_table_seq,
                    source_snapshot_location: source_snapshot_location.clone(),
                    definition_output_columns: definition_context
                        .columns
                        .iter()
                        .map(|column| column.index)
                        .collect(),
                    read_output_columns: read_context
                        .columns
                        .iter()
                        .map(|column| column.index)
                        .collect(),
                });
            }
            if !candidates.is_empty() {
                metadata
                    .write()
                    .add_materialized_view_candidates(source_table_id, candidates);
            }
        }
        Ok(())
    }

    fn check_materialized_view_license(&self) -> Result<()> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::MaterializedView)
    }

    async fn resolve_materialized_view_target(
        &self,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        view: &Identifier,
    ) -> Result<(String, String, String, Arc<dyn Table>)> {
        let (catalog, database, view) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let table = self.ctx.get_table(&catalog, &database, &view).await?;
        if !is_materialized_view_engine(table.engine()) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "'{}.{}.{}' is not a materialized view",
                catalog, database, view
            )));
        }
        Ok((catalog, database, view, table))
    }

    fn materialized_view_cluster_schema(physical_schema: &TableSchemaRef) -> TableSchemaRef {
        let fields = physical_schema
            .fields()
            .iter()
            // Aggregate states use Tuple physical types and are rejected by the existing Fuse
            // cluster-key type validation. The refresh row ID is a String, so exclude that hidden
            // physical column before delegating to the same validation.
            .filter(|field| field.name() != MATERIALIZED_VIEW_SOURCE_ROW_ID_COLUMN)
            .cloned()
            .collect();
        TableSchemaRefExt::create(fields)
    }

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
                if !matches!(
                    function.return_type.remove_nullable(),
                    DataType::AggregateState(_)
                ) {
                    return Err(ErrorCode::Internal(format!(
                        "materialized view state function '{}' did not produce AggregateState",
                        function.func_name
                    )));
                }
                data_types.push(infer_schema_type(function.return_type.as_ref())?);
            }
            for item in &aggregate.group_items {
                data_types.push(infer_schema_type(item.scalar.data_type().as_ref())?);
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
                let field = TableField::new(name, field.data_type().clone());
                if define_expr == name {
                    field
                } else {
                    field.with_default_expr(Some(define_expr.clone()))
                }
            })
            .collect();
        Ok(TableSchemaRefExt::create(fields))
    }

    /// Resolve the single base table referenced by the defining query.
    fn materialized_view_source_table(metadata: &MetadataRef) -> Result<TableEntry> {
        let metadata = metadata.read();
        if metadata.tables().len() != 1 {
            return Err(ErrorCode::SemanticError(
                "Materialized view requires exactly one base table source".to_string(),
            ));
        }
        Ok(metadata.tables()[0].clone())
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_materialized_view(
        &mut self,
        stmt: &CreateMaterializedViewStmt,
    ) -> Result<Plan> {
        self.check_materialized_view_license()?;

        let CreateMaterializedViewStmt {
            create_option,
            catalog,
            database,
            view,
            columns,
            cluster_by,
            comment,
            table_options,
            query,
        } = stmt;

        if cluster_by
            .as_ref()
            .is_some_and(|cluster_by| cluster_by.cluster_type == AstClusterType::Hilbert)
        {
            return Err(ErrorCode::Unimplemented(
                "Hilbert clustering is not supported for materialized views".to_string(),
            ));
        }
        if cluster_by.is_some() && columns.is_empty() {
            return Err(ErrorCode::SemanticError(
                "Materialized view with CLUSTER BY must include a column list".to_string(),
            ));
        }

        // Reject unsupported syntax before binding. The rest of CREATE can then rely on a single
        // plain table source and on the SELECT/FROM/WHERE/GROUP BY shape understood by refresh.
        let checker = MaterializedViewChecker::check_query(query);
        if !checker.is_supported() {
            return Err(ErrorCode::SemanticError(format!(
                "Materialized View only supports simple SELECT queries over one table, with optional WHERE/GROUP BY clauses, registered aggregate functions, and deterministic expressions",
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

        let direct_source = match &query.body {
            SetExpr::Select(select) => match select.from.as_slice() {
                [TableReference::Table { table, .. }] => table,
                _ => {
                    return Err(ErrorCode::InvalidMaterializedView(
                        "Materialized view requires exactly one base table source".to_string(),
                    ));
                }
            },
            _ => {
                return Err(ErrorCode::InvalidMaterializedView(
                    "Materialized view definition must be a SELECT query".to_string(),
                ));
            }
        };
        let (direct_source_catalog, direct_source_database, direct_source_name) = self
            .normalize_object_identifier_triple(
                &direct_source.catalog,
                &direct_source.database,
                &direct_source.table,
            );
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
        let source_entry = Self::materialized_view_source_table(original_metadata)?;
        let source_table = source_entry.table();
        // CREATE records source_entry, not the FROM name in SQL. Binding a stale MV falls back to
        // its original query, so metadata contains only the underlying FUSE table. That leftover
        // table passes the engine check; comparing it with the AST name rejects CREATE FROM mv.
        if direct_source_catalog != source_entry.catalog()
            || direct_source_database != source_entry.database()
            || direct_source_name != source_entry.name()
            || !source_entry.catalog().eq_ignore_ascii_case(CATALOG_DEFAULT)
            || !is_supported_materialized_view_source(source_table.as_ref())
        {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Materialized view source '{}.{}.{}' must be a persistent base table in the default catalog using FUSE engine",
                direct_source_catalog, direct_source_database, direct_source_name
            )));
        }
        let source_catalog_name = source_entry.catalog().to_string();
        let source_database = source_entry.database().to_string();
        let source_table_name = source_entry.name().to_string();
        let source_table_id = source_table.get_id();
        let source_table_seq = source_table.get_table_info().ident.seq;
        let source_database_id = source_table
            .get_table_info()
            .meta
            .options
            .get(OPT_KEY_DATABASE_ID)
            .ok_or_else(|| {
                ErrorCode::InvalidMaterializedView(format!(
                    "source table id {source_table_id} does not record its database id"
                ))
            })?
            .parse::<u64>()?;
        if source_catalog_name != catalog_name || source_database_id != target_database_id {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view and source table must belong to the same catalog and database: target '{}.{}', source '{}.{}'",
                catalog_name, database_name, source_catalog_name, source_database
            )));
        }
        let source_table_option =
            (!source_table.change_tracking_enabled()).then(|| UpsertTableOptionReq {
                table_id: source_table_id,
                seq: MatchSeq::Exact(source_table_seq),
                options: HashMap::from([
                    (
                        OPT_KEY_CHANGE_TRACKING.to_string(),
                        Some("true".to_string()),
                    ),
                    (
                        OPT_KEY_CHANGE_TRACKING_BEGIN_VER.to_string(),
                        Some(source_table_seq.to_string()),
                    ),
                ]),
            });
        let source_catalog = self.ctx.get_catalog(&source_catalog_name).await?;
        let expected_source_generation = get_materialized_view_handler()
            .get_mv_current_source_generation(source_catalog.as_ref(), &tenant, source_table_id)
            .await?
            .unwrap_or(0);

        // The source option update is intentionally deferred to CREATE so publishing the MV and
        // enabling CHANGE_TRACKING remain atomic. Bind the physical definition against an exact,
        // Binder-local clone that reflects that pending option; otherwise change$row_id would be
        // rejected before CREATE gets a chance to commit the source update.
        let physical_source = if source_table_option.is_some() {
            let mut source_info = source_table.get_table_info().clone();
            source_info
                .meta
                .options
                .insert(OPT_KEY_CHANGE_TRACKING.to_string(), "true".to_string());
            source_info.meta.options.insert(
                OPT_KEY_CHANGE_TRACKING_BEGIN_VER.to_string(),
                source_table_seq.to_string(),
            );
            source_catalog.get_table_by_info(&source_info)?
        } else {
            source_table.clone()
        };

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
        physical_binder.pre_resolved_tables.insert(
            (
                source_catalog_name,
                source_database.clone(),
                source_table_name.clone(),
            ),
            physical_source,
        );
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
        for (key, value) in table_options {
            self.insert_table_option_with_validation(
                &mut options,
                key.to_lowercase(),
                value.clone(),
            )?;
        }
        if let Some(compression) = options.get(OPT_KEY_TABLE_COMPRESSION) {
            let _: TableCompression = compression.as_str().try_into()?;
        }
        if let Some(mode) = options.get(OPT_KEY_WRITE_DISTRIBUTION_MODE) {
            let mode = mode.parse::<WriteDistributionMode>()?;
            if mode == WriteDistributionMode::Hash {
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "{OPT_KEY_WRITE_DISTRIBUTION_MODE}='hash' requires PARTITION BY"
                )));
            }
        }
        options.insert(
            OPT_KEY_DATABASE_ID.to_owned(),
            target_database_id.to_string(),
        );
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_ID.to_owned(),
            source_table_id.to_string(),
        );
        options.insert(
            OPT_KEY_MATERIALIZED_VIEW_SOURCE_TABLE_NAME.to_owned(),
            source_table_name,
        );
        // CreateTableInterpreter removes this transport option and stores it in TableMeta.comment;
        // ALTER MATERIALIZED VIEW SET OPTIONS does not accept COMMENT.
        if let Some(comment) = comment {
            options.insert(OPT_KEY_COMMENT.to_owned(), comment.clone());
        }

        let mut cluster_key = None;
        if let Some(cluster_opt) = cluster_by {
            let cluster_schema = Self::materialized_view_cluster_schema(&physical_schema);
            let keys = self
                .analyze_cluster_keys(cluster_opt, cluster_schema, None, true)
                .await
                .map_err(|error| {
                    ErrorCode::InvalidClusterKeys(format!(
                        "materialized view CLUSTER BY must reference physical non-aggregate columns or GROUP BY keys: {error}"
                    ))
                })?;
            if !keys.is_empty() {
                options
                    .entry(OPT_KEY_AGGRESSIVE_RECLUSTER.to_owned())
                    .or_insert_with(|| "1".to_owned());
                cluster_key = Some(format!("({})", keys.join(", ")));
            }
        }

        // TODO: resolve the database default storage params for the materialized view so it honors
        // DEFAULT_STORAGE_CONNECTION/DEFAULT_STORAGE_PATH like base tables do. Deferred to a
        // follow-up; the MV currently falls back to the global default storage.
        let storage_params = None;

        let mv_definition = MVDefinition {
            original_query: original_query.to_string(),
            query: physical_query.to_string(),
            logical_schema: logical_schema.as_ref().clone(),
            sync_creation: false,
        };

        let table_plan = CreateTablePlan {
            create_option: create_option.clone().into(),
            tenant,
            catalog: catalog_name,
            database: database_name,
            table: view_name,
            schema: physical_schema,
            engine: Engine::MaterializedView,
            engine_options: Default::default(),
            storage_params,
            options,
            table_properties: None,
            table_partition: None,
            field_comments: vec![],
            field_stats_truncate_len: vec![],
            cluster_key,
            as_select: None,
            table_indexes: None,
            table_constraints: None,
            attached_columns: None,
        };
        let plan = CreateMaterializedViewPlan {
            table_plan,
            mv_definition,
            source_table_option,
            query_plan: Box::new(original_query_plan),
            expected_source_generation,
        };

        Ok(Plan::CreateMaterializedView(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_alter_materialized_view(
        &mut self,
        stmt: &AlterMaterializedViewStmt,
    ) -> Result<Plan> {
        self.check_materialized_view_license()?;

        let AlterMaterializedViewStmt {
            catalog,
            database,
            view,
            action,
        } = stmt;
        let tenant = self.ctx.get_tenant();
        let (catalog, database, table, mv_table) = self
            .resolve_materialized_view_target(catalog, database, view)
            .await?;
        let table_id = mv_table.get_id();

        match action {
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                if cluster_by.cluster_type == AstClusterType::Hilbert {
                    return Err(ErrorCode::Unimplemented(
                        "Hilbert clustering is not supported for materialized views".to_string(),
                    ));
                }
                let cluster_schema = Self::materialized_view_cluster_schema(&mv_table.schema());
                let cluster_keys = self
                    .analyze_cluster_keys(cluster_by, cluster_schema, None, true)
                    .await
                    .map_err(|error| {
                        ErrorCode::InvalidClusterKeys(format!(
                            "materialized view CLUSTER BY must reference physical non-aggregate columns or GROUP BY keys: {error}"
                        ))
                    })?;

                Ok(Plan::AlterTableClusterKey(Box::new(
                    AlterTableClusterKeyPlan {
                        tenant,
                        catalog,
                        database,
                        table,
                        target: MaintenanceTarget::MaterializedView { table_id },
                        branch: None,
                        cluster_keys,
                        cluster_type: cluster_by.cluster_type.to_string().parse()?,
                    },
                )))
            }
            AlterTableAction::DropTableClusterKey => Ok(Plan::DropTableClusterKey(Box::new(
                DropTableClusterKeyPlan {
                    tenant,
                    catalog,
                    database,
                    table,
                    target: MaintenanceTarget::MaterializedView { table_id },
                    branch: None,
                },
            ))),
            AlterTableAction::ReclusterTable {
                is_final,
                selection,
                limit,
            } => {
                if selection.is_some() {
                    return Err(ErrorCode::Unimplemented(
                        "ALTER MATERIALIZED VIEW RECLUSTER WHERE is not supported".to_string(),
                    ));
                }
                Ok(Plan::ReclusterTable(Box::new(ReclusterPlan {
                    catalog,
                    database,
                    table,
                    target: MaintenanceTarget::MaterializedView { table_id },
                    limit: limit.map(|value| value as usize),
                    selection: None,
                    is_final: *is_final,
                })))
            }
            AlterTableAction::SetOptions { set_options } => {
                Ok(Plan::SetOptions(Box::new(SetOptionsPlan {
                    set_options: set_options.clone(),
                    catalog,
                    database,
                    table,
                    target: MaintenanceTarget::MaterializedView { table_id },
                })))
            }
            AlterTableAction::UnsetOptions { targets } => {
                Ok(Plan::UnsetOptions(Box::new(UnsetOptionsPlan {
                    options: targets.iter().map(|i| i.name.to_lowercase()).collect(),
                    catalog,
                    database,
                    table,
                    target: MaintenanceTarget::MaterializedView { table_id },
                })))
            }
            AlterTableAction::ModifyTableComment { new_comment } => {
                Ok(Plan::ModifyTableComment(Box::new(ModifyTableCommentPlan {
                    if_exists: false,
                    new_comment: new_comment.to_string(),
                    catalog,
                    database,
                    table,
                    target: MaintenanceTarget::MaterializedView { table_id },
                })))
            }
            _ => Err(ErrorCode::SemanticError(format!(
                "unsupported ALTER MATERIALIZED VIEW action: {action}"
            ))),
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_materialized_view(
        &mut self,
        stmt: &DropMaterializedViewStmt,
    ) -> Result<Plan> {
        self.check_materialized_view_license()?;

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
        self.check_materialized_view_license()?;

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
    pub(crate) async fn bind_show_create_materialized_view(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &ShowCreateMaterializedViewStmt,
    ) -> Result<Plan> {
        self.check_materialized_view_license()?;

        let ShowCreateMaterializedViewStmt {
            catalog,
            database,
            view,
        } = stmt;
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", DataType::String),
            DataField::new("Create Table", DataType::String),
        ]);
        Ok(Plan::ShowCreateMaterializedView(Box::new(
            ShowCreateMaterializedViewPlan {
                catalog,
                database,
                view_name,
                schema,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_materialized_views(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowMaterializedViewsStmt,
    ) -> Result<Plan> {
        self.check_materialized_view_license()?;

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
            "{}.system.materialized_views",
            QuotedIdent(catalog_name.to_lowercase(), '`')
        ));
        select_builder
            .with_column("created_on")
            .with_column("name")
            .with_column("catalog")
            .with_column("database")
            .with_column("cluster_by")
            .with_column("num_rows")
            .with_column("data_size")
            .with_column("data_compressed_size")
            .with_column("source_catalog")
            .with_column("source_database")
            .with_column("source_table")
            .with_column("invalid")
            .with_column("invalid_reason")
            .with_column("comment")
            .with_column("text");

        select_builder.with_filter(format!("database = {}", QuotedString(&database_name, '\'')));
        select_builder.with_filter(format!("catalog = {}", QuotedString(&catalog_name, '\'')));

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
            RewriteKind::ShowMaterializedViews,
        )
        .await
    }
}
