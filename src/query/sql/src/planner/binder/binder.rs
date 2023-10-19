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
use std::str::FromStr;
use std::sync::Arc;

use chrono_tz::Tz;
use common_ast::ast::format_statement;
use common_ast::ast::ExplainKind;
use common_ast::ast::Hint;
use common_ast::ast::Identifier;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_expression::Expr;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::principal::StageFileFormatType;
use indexmap::IndexMap;
use log::warn;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::binder::CteInfo;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::CreateFileFormatPlan;
use crate::plans::CreateRolePlan;
use crate::plans::DropFileFormatPlan;
use crate::plans::DropRolePlan;
use crate::plans::DropStagePlan;
use crate::plans::DropUDFPlan;
use crate::plans::DropUserPlan;
use crate::plans::MaterializedCte;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::RewriteKind;
use crate::plans::ShowFileFormatsPlan;
use crate::plans::ShowGrantsPlan;
use crate::plans::ShowRolesPlan;
use crate::plans::UseDatabasePlan;
use crate::BindContext;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarExpr;
use crate::TypeChecker;
use crate::Visibility;

/// Binder is responsible to transform AST of a query into a canonical logical SExpr.
///
/// During this phase, it will:
/// - Resolve columns and tables with Catalog
/// - Check semantic of query
/// - Validate expressions
/// - Build `Metadata`
pub struct Binder {
    pub ctx: Arc<dyn TableContext>,
    pub catalogs: Arc<CatalogManager>,
    pub name_resolution_ctx: NameResolutionContext,
    pub metadata: MetadataRef,
    // Save the equal scalar exprs for joins
    // Eg: SELECT * FROM (twocolumn AS a JOIN twocolumn AS b USING(x) JOIN twocolumn AS c on a.x = c.x) ORDER BY x LIMIT 1
    // The eq_scalars is [(a.x, b.x), (a.x, c.x)]
    pub eq_scalars: Vec<(ScalarExpr, ScalarExpr)>,
    // Save the bound context for materialized cte, the key is cte_idx
    pub m_cte_bound_ctx: HashMap<IndexType, BindContext>,
    pub m_cte_bound_s_expr: HashMap<IndexType, SExpr>,
    /// Use `IndexMap` because need to keep the insertion order
    /// Then wrap materialized ctes to main plan.
    pub ctes_map: Box<IndexMap<String, CteInfo>>,
}

impl<'a> Binder {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        catalogs: Arc<CatalogManager>,
        name_resolution_ctx: NameResolutionContext,
        metadata: MetadataRef,
    ) -> Self {
        Binder {
            ctx,
            catalogs,
            name_resolution_ctx,
            metadata,
            m_cte_bound_ctx: Default::default(),
            eq_scalars: vec![],
            m_cte_bound_s_expr: Default::default(),
            ctes_map: Box::default(),
        }
    }

    // After the materialized cte was bound, add it to `m_cte_bound_ctx`
    pub fn set_m_cte_bound_ctx(&mut self, cte_idx: IndexType, bound_ctx: BindContext) {
        self.m_cte_bound_ctx.insert(cte_idx, bound_ctx);
    }

    pub fn set_m_cte_bound_s_expr(&mut self, cte_idx: IndexType, s_expr: SExpr) {
        self.m_cte_bound_s_expr.insert(cte_idx, s_expr);
    }

    #[async_backtrace::framed]
    pub async fn bind(mut self, stmt: &Statement) -> Result<Plan> {
        self.ctx.set_status_info("binding");
        let mut init_bind_context = BindContext::new();
        let plan = self.bind_statement(&mut init_bind_context, stmt).await?;
        self.bind_query_index(&mut init_bind_context, &plan).await?;
        Ok(plan)
    }

    pub(crate) async fn opt_hints_set_var(
        &mut self,
        bind_context: &mut BindContext,
        hints: &Hint,
    ) -> Result<()> {
        let mut type_checker = TypeChecker::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            false,
            false,
        );
        let mut hint_settings: HashMap<String, String> = HashMap::new();
        for hint in &hints.hints_list {
            let variable = &hint.name.name;
            let (scalar, _) = *type_checker.resolve(&hint.expr).await?;

            let scalar = wrap_cast(&scalar, &DataType::String);
            let expr = scalar.as_expr()?;

            let (new_expr, _) =
                ConstantFolder::fold(&expr, &self.ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
            match new_expr {
                Expr::Constant { scalar, .. } => {
                    let value = String::from_utf8(scalar.into_string().unwrap())?;
                    if variable.to_lowercase().as_str() == "timezone" {
                        let tz = value.trim_matches(|c| c == '\'' || c == '\"');
                        tz.parse::<Tz>().map_err(|_| {
                            ErrorCode::InvalidTimezone(format!("Invalid Timezone: {:?}", value))
                        })?;
                    }
                    hint_settings.entry(variable.to_string()).or_insert(value);
                }
                _ => {
                    warn!("fold hints {:?} failed. value must be constant value", hint);
                }
            }
        }

        self.ctx.get_settings().set_batch_settings(&hint_settings)
    }

    #[async_recursion::async_recursion]
    #[async_backtrace::framed]
    pub(crate) async fn bind_statement(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &Statement,
    ) -> Result<Plan> {
        let plan = match stmt {
            Statement::Query(query) => {
                let (mut s_expr, bind_context) = self.bind_query(bind_context, query).await?;
                // Wrap `LogicalMaterializedCte` to `s_expr`
                for (_, cte_info) in self.ctes_map.iter().rev() {
                    if !cte_info.materialized || cte_info.used_count == 0 {
                        continue;
                    }
                    let cte_s_expr = self.m_cte_bound_s_expr.get(&cte_info.cte_idx).unwrap();
                    let left_output_columns = cte_info.columns.clone();
                    s_expr = SExpr::create_binary(
                        Arc::new(RelOperator::MaterializedCte(MaterializedCte { left_output_columns, cte_idx: cte_info.cte_idx })),
                        Arc::new(cte_s_expr.clone()),
                        Arc::new(s_expr),
                    );
                }
                let formatted_ast = if self.ctx.get_settings().get_enable_query_result_cache()? {
                    Some(format_statement(stmt.clone())?)
                } else {
                    None
                };
                Plan::Query {
                    s_expr: Box::new(s_expr),
                    metadata: self.metadata.clone(),
                    bind_context: Box::new(bind_context),
                    rewrite_kind: None,
                    ignore_result: query.ignore_result,
                    formatted_ast,
                }
            }

            Statement::Explain { query, kind } => {
                match kind {
                    ExplainKind::Ast(formatted_stmt) => Plan::ExplainAst { formatted_string: formatted_stmt.clone() },
                    ExplainKind::Syntax(formatted_sql) => Plan::ExplainSyntax { formatted_sql: formatted_sql.clone() },
                    _ => Plan::Explain { kind: kind.clone(), plan: Box::new(self.bind_statement(bind_context, query).await?) },
                }
            }

            Statement::ExplainAnalyze { query } => {
                let plan = self.bind_statement(bind_context, query).await?;
                Plan::ExplainAnalyze { plan: Box::new(plan) }
            }

            Statement::ShowFunctions { limit } => {
                self.bind_show_functions(bind_context, limit).await?
            }

            Statement::ShowTableFunctions { limit } => {
                self.bind_show_table_functions(bind_context, limit).await?
            }

            Statement::CopyIntoTable(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In Copy resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_copy_into_table(bind_context, stmt).await?
            }

            Statement::CopyIntoLocation(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In Copy resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_copy_into_location(bind_context, stmt).await?
            }

            Statement::ShowMetrics => {
                self.bind_rewrite_to_query(
                    bind_context,
                    "SELECT metric, kind, labels, value FROM system.metrics",
                    RewriteKind::ShowMetrics,
                )
                    .await?
            }
            Statement::ShowProcessList => {
                self.bind_rewrite_to_query(bind_context, "SELECT * FROM system.processes", RewriteKind::ShowProcessList)
                    .await?
            }
            Statement::ShowEngines => {
                self.bind_rewrite_to_query(bind_context, "SELECT \"Engine\", \"Comment\" FROM system.engines ORDER BY \"Engine\" ASC", RewriteKind::ShowEngines)
                    .await?
            }
            Statement::ShowSettings { like } => self.bind_show_settings(bind_context, like).await?,
            Statement::ShowIndexes => {
                self.bind_rewrite_to_query(bind_context, "SELECT * FROM system.indexes", RewriteKind::ShowProcessList)
                    .await?
            }
            // Catalogs
            Statement::ShowCatalogs(stmt) => self.bind_show_catalogs(bind_context, stmt).await?,
            Statement::ShowCreateCatalog(stmt) => self.bind_show_create_catalogs(stmt).await?,
            Statement::CreateCatalog(stmt) => self.bind_create_catalog(stmt).await?,
            Statement::DropCatalog(stmt) => self.bind_drop_catalog(stmt).await?,

            // Databases
            Statement::ShowDatabases(stmt) => self.bind_show_databases(bind_context, stmt).await?,
            Statement::ShowCreateDatabase(stmt) => self.bind_show_create_database(stmt).await?,
            Statement::CreateDatabase(stmt) => self.bind_create_database(stmt).await?,
            Statement::DropDatabase(stmt) => self.bind_drop_database(stmt).await?,
            Statement::UndropDatabase(stmt) => self.bind_undrop_database(stmt).await?,
            Statement::AlterDatabase(stmt) => self.bind_alter_database(stmt).await?,
            Statement::UseDatabase { database } => {
                let database = normalize_identifier(database, &self.name_resolution_ctx).name;
                Plan::UseDatabase(Box::new(UseDatabasePlan {
                    database,
                }))
            }
            // Columns
            Statement::ShowColumns(stmt) => self.bind_show_columns(bind_context, stmt).await?,
            // Tables
            Statement::ShowTables(stmt) => self.bind_show_tables(bind_context, stmt).await?,
            Statement::ShowCreateTable(stmt) => self.bind_show_create_table(stmt).await?,
            Statement::DescribeTable(stmt) => self.bind_describe_table(stmt).await?,
            Statement::ShowTablesStatus(stmt) => {
                self.bind_show_tables_status(bind_context, stmt).await?
            }
            Statement::ShowDropTables(stmt) => {
                self.bind_show_drop_tables(bind_context, stmt).await?
            }
            Statement::AttachTable(stmt) => self.bind_attach_table(stmt).await?,
            Statement::CreateTable(stmt) => self.bind_create_table(stmt).await?,
            Statement::DropTable(stmt) => self.bind_drop_table(stmt).await?,
            Statement::UndropTable(stmt) => self.bind_undrop_table(stmt).await?,
            Statement::AlterTable(stmt) => self.bind_alter_table(bind_context, stmt).await?,
            Statement::RenameTable(stmt) => self.bind_rename_table(stmt).await?,
            Statement::TruncateTable(stmt) => self.bind_truncate_table(stmt).await?,
            Statement::OptimizeTable(stmt) => self.bind_optimize_table(bind_context, stmt).await?,
            Statement::VacuumTable(stmt) => self.bind_vacuum_table(bind_context, stmt).await?,
            Statement::VacuumDropTable(stmt) => self.bind_vacuum_drop_table(bind_context, stmt).await?,
            Statement::AnalyzeTable(stmt) => self.bind_analyze_table(stmt).await?,
            Statement::ExistsTable(stmt) => self.bind_exists_table(stmt).await?,

            // Views
            Statement::CreateView(stmt) => self.bind_create_view(stmt).await?,
            Statement::AlterView(stmt) => self.bind_alter_view(stmt).await?,
            Statement::DropView(stmt) => self.bind_drop_view(stmt).await?,

            // Indexes
            Statement::CreateIndex(stmt) => self.bind_create_index(bind_context, stmt).await?,
            Statement::DropIndex(stmt) => self.bind_drop_index(stmt).await?,
            Statement::RefreshIndex(stmt) => self.bind_refresh_index(bind_context, stmt).await?,

            // Virtual Columns
            Statement::CreateVirtualColumn(stmt) => self.bind_create_virtual_column(stmt).await?,
            Statement::AlterVirtualColumn(stmt) => self.bind_alter_virtual_column(stmt).await?,
            Statement::DropVirtualColumn(stmt) => self.bind_drop_virtual_column(stmt).await?,
            Statement::RefreshVirtualColumn(stmt) => self.bind_refresh_virtual_column(stmt).await?,

            // Users
            Statement::CreateUser(stmt) => self.bind_create_user(stmt).await?,
            Statement::DropUser { if_exists, user } => Plan::DropUser(Box::new(DropUserPlan {
                if_exists: *if_exists,
                user: user.clone(),
            })),
            Statement::ShowUsers => self.bind_rewrite_to_query(bind_context, "SELECT name, hostname, auth_type, is_configured FROM system.users ORDER BY name", RewriteKind::ShowUsers).await?,
            Statement::AlterUser(stmt) => self.bind_alter_user(stmt).await?,

            // Roles
            Statement::ShowRoles => Plan::ShowRoles(Box::new(ShowRolesPlan {})),
            Statement::CreateRole {
                if_not_exists,
                role_name,
            } => Plan::CreateRole(Box::new(CreateRolePlan {
                if_not_exists: *if_not_exists,
                role_name: role_name.to_string(),
            })),
            Statement::DropRole {
                if_exists,
                role_name,
            } => Plan::DropRole(Box::new(DropRolePlan {
                if_exists: *if_exists,
                role_name: role_name.to_string(),
            })),

            // Stages
            Statement::ShowStages => self.bind_rewrite_to_query(bind_context, "SELECT name, stage_type, number_of_files, creator, comment FROM system.stages ORDER BY name", RewriteKind::ShowStages).await?,
            Statement::ListStage { location, pattern } => {
                let pattern = if let Some(pattern) = pattern {
                    format!(", pattern => '{pattern}'")
                } else {
                    "".to_string()
                };
                self.bind_rewrite_to_query(bind_context, format!("SELECT * FROM LIST_STAGE(location => '@{location}'{pattern})").as_str(), RewriteKind::ListStage).await?
            }
            Statement::DescribeStage { stage_name } => self.bind_rewrite_to_query(bind_context, format!("SELECT * FROM system.stages WHERE name = '{stage_name}'").as_str(), RewriteKind::DescribeStage).await?,
            Statement::CreateStage(stmt) => self.bind_create_stage(stmt).await?,
            Statement::DropStage {
                stage_name,
                if_exists,
            } => Plan::DropStage(Box::new(DropStagePlan {
                if_exists: *if_exists,
                name: stage_name.clone(),
            })),
            Statement::RemoveStage { location, pattern } => {
                self.bind_remove_stage(location, pattern).await?
            }
            Statement::Insert(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In INSERT resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_insert(bind_context, stmt).await?
            }
            Statement::Replace(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In REPLACE resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_replace(bind_context, stmt).await?
            }
            Statement::MergeInto(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In Merge resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_merge_into(bind_context, stmt).await?
            }
            Statement::Delete {
                hints,
                table_reference,
                selection,
            } => {
                if let Some(hints) = hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In DELETE resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_delete(bind_context, table_reference, selection)
                    .await?
            }
            Statement::Update(stmt) => {
                if let Some(hints) = &stmt.hints {
                    if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                        warn!("In UPDATE resolve optimize hints {:?} failed, err: {:?}", hints, e);
                    }
                }
                self.bind_update(bind_context, stmt).await?
            }

            // Permissions
            Statement::Grant(stmt) => self.bind_grant(stmt).await?,
            Statement::ShowGrants { principal } => Plan::ShowGrants(Box::new(ShowGrantsPlan {
                principal: principal.clone(),
            })),
            Statement::Revoke(stmt) => self.bind_revoke(stmt).await?,

            // File Formats
            Statement::CreateFileFormat { if_not_exists, name, file_format_options } => {
                if StageFileFormatType::from_str(name).is_ok() {
                    return Err(ErrorCode::SyntaxException(format!(
                        "File format {name} is reserved"
                    )));
                }
                Plan::CreateFileFormat(Box::new(CreateFileFormatPlan {
                    if_not_exists: *if_not_exists,
                    name: name.clone(),
                    file_format_params: file_format_options.clone().try_into()?,
                }))
            }

            Statement::DropFileFormat {
                if_exists,
                name,
            } => Plan::DropFileFormat(Box::new(DropFileFormatPlan {
                if_exists: *if_exists,
                name: name.clone(),
            })),
            Statement::ShowFileFormats => Plan::ShowFileFormats(Box::new(ShowFileFormatsPlan {})),

            // UDFs
            Statement::CreateUDF(stmt) => self.bind_create_udf(stmt).await?,
            Statement::AlterUDF(stmt) => self.bind_alter_udf(stmt).await?,
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => Plan::DropUDF(Box::new(DropUDFPlan {
                if_exists: *if_exists,
                name: udf_name.to_string(),
            })),
            Statement::Call(stmt) => self.bind_call(bind_context, stmt).await?,

            Statement::Presign(stmt) => self.bind_presign(bind_context, stmt).await?,

            Statement::SetVariable {
                is_global,
                variable,
                value,
            } => {
                self.bind_set_variable(bind_context, *is_global, variable, value)
                    .await?
            }

            Statement::UnSetVariable(stmt) => {
                self.bind_unset_variable(bind_context, stmt)
                    .await?
            }

            Statement::SetRole {
                is_default,
                role_name,
            } => {
                self.bind_set_role(bind_context, *is_default, role_name).await?
            }

            Statement::KillStmt { kill_target, object_id } => {
                self.bind_kill_stmt(bind_context, kill_target, object_id.as_str())
                    .await?
            }

            // share statements
            Statement::CreateShareEndpoint(stmt) => {
                self.bind_create_share_endpoint(stmt).await?
            }
            Statement::ShowShareEndpoint(stmt) => {
                self.bind_show_share_endpoint(stmt).await?
            }
            Statement::DropShareEndpoint(stmt) => {
                self.bind_drop_share_endpoint(stmt).await?
            }
            Statement::CreateShare(stmt) => {
                self.bind_create_share(stmt).await?
            }
            Statement::DropShare(stmt) => {
                self.bind_drop_share(stmt).await?
            }
            Statement::GrantShareObject(stmt) => {
                self.bind_grant_share_object(stmt).await?
            }
            Statement::RevokeShareObject(stmt) => {
                self.bind_revoke_share_object(stmt).await?
            }
            Statement::AlterShareTenants(stmt) => {
                self.bind_alter_share_accounts(stmt).await?
            }
            Statement::DescShare(stmt) => {
                self.bind_desc_share(stmt).await?
            }
            Statement::ShowShares(stmt) => {
                self.bind_show_shares(stmt).await?
            }
            Statement::ShowObjectGrantPrivileges(stmt) => {
                self.bind_show_object_grant_privileges(stmt).await?
            }
            Statement::ShowGrantsOfShare(stmt) => {
                self.bind_show_grants_of_share(stmt).await?
            }
            Statement::CreateDatamaskPolicy(stmt) => {
                self.bind_create_data_mask_policy(stmt).await?
            }
            Statement::DropDatamaskPolicy(stmt) => {
                self.bind_drop_data_mask_policy(stmt).await?
            }
            Statement::DescDatamaskPolicy(stmt) => {
                self.bind_desc_data_mask_policy(stmt).await?
            }
            Statement::CreateNetworkPolicy(stmt) => {
                self.bind_create_network_policy(stmt).await?
            }
            Statement::AlterNetworkPolicy(stmt) => {
                self.bind_alter_network_policy(stmt).await?
            }
            Statement::DropNetworkPolicy(stmt) => {
                self.bind_drop_network_policy(stmt).await?
            }
            Statement::DescNetworkPolicy(stmt) => {
                self.bind_desc_network_policy(stmt).await?
            }
            Statement::ShowNetworkPolicies => {
                self.bind_show_network_policies().await?
            }
            Statement::CreateTask(stmt) => {
                self.bind_create_task(stmt).await?
            }
        };
        Ok(plan)
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_rewrite_to_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &str,
        rewrite_kind_r: RewriteKind,
    ) -> Result<Plan> {
        let tokens = tokenize_sql(query)?;
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
        let mut plan = self.bind_statement(bind_context, &stmt).await?;

        if let Plan::Query { rewrite_kind, .. } = &mut plan {
            *rewrite_kind = Some(rewrite_kind_r)
        }
        Ok(plan)
    }

    /// Create a new ColumnBinding for derived column
    pub(crate) fn create_derived_column_binding(
        &mut self,
        column_name: String,
        data_type: DataType,
    ) -> ColumnBinding {
        let index = self
            .metadata
            .write()
            .add_derived_column(column_name.clone(), data_type.clone());
        ColumnBindingBuilder::new(column_name, index, Box::new(data_type), Visibility::Visible)
            .build()
    }

    /// Normalize [[<catalog>].<database>].<object>
    /// object like table, view ...
    pub fn normalize_object_identifier_triple(
        &self,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        object: &Identifier,
    ) -> (String, String, String) {
        let catalog_name = catalog
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database_name = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let object_name = normalize_identifier(object, &self.name_resolution_ctx).name;
        (catalog_name, database_name, object_name)
    }

    /// Normalize <identifier>
    pub fn normalize_object_identifier(&self, ident: &Identifier) -> String {
        normalize_identifier(ident, &self.name_resolution_ctx).name
    }

    pub fn judge_equal_scalars(&self, left: &ScalarExpr, right: &ScalarExpr) -> bool {
        self.eq_scalars
            .iter()
            .any(|(l, r)| (l == left && r == right) || (l == right && r == left))
    }
}
