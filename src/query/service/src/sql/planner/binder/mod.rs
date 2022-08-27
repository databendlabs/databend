// Copyright 2022 Datafuse Labs.
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

pub use aggregate::AggregateInfo;
pub use bind_context::*;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_ast::UDFValidator;
use common_datavalues::DataTypeImpl;
use common_exception::Result;
use common_meta_types::UserDefinedFunction;
use common_planners::AlterUserUDFPlan;
use common_planners::CallPlan;
use common_planners::CreateRolePlan;
use common_planners::CreateUserUDFPlan;
use common_planners::DropRolePlan;
use common_planners::DropUserPlan;
use common_planners::DropUserStagePlan;
use common_planners::DropUserUDFPlan;
use common_planners::ShowGrantsPlan;
use common_planners::UseDatabasePlan;
pub use scalar::ScalarBinder;
pub use scalar_common::*;

use super::plans::Plan;
use super::plans::RewriteKind;
use super::semantic::NameResolutionContext;
use crate::catalogs::CatalogManager;
use crate::sessions::QueryContext;
use crate::sql::planner::metadata::MetadataRef;

mod aggregate;
mod bind_context;
mod copy;
mod ddl;
mod delete;
mod distinct;
mod having;
mod insert;
mod join;
mod kill;
mod limit;
mod presign;
mod project;
mod scalar;
mod scalar_common;
mod scalar_visitor;
mod select;
mod setting;
mod show;
mod sort;
mod table;

/// Binder is responsible to transform AST of a query into a canonical logical SExpr.
///
/// During this phase, it will:
/// - Resolve columns and tables with Catalog
/// - Check semantic of query
/// - Validate expressions
/// - Build `Metadata`
pub struct Binder {
    ctx: Arc<QueryContext>,
    catalogs: Arc<CatalogManager>,
    name_resolution_ctx: NameResolutionContext,
    metadata: MetadataRef,
}

impl<'a> Binder {
    pub fn new(
        ctx: Arc<QueryContext>,
        catalogs: Arc<CatalogManager>,
        name_resolution_ctx: NameResolutionContext,
        metadata: MetadataRef,
    ) -> Self {
        Binder {
            ctx,
            catalogs,
            name_resolution_ctx,
            metadata,
        }
    }

    pub async fn bind(mut self, stmt: &Statement<'a>) -> Result<Plan> {
        let init_bind_context = BindContext::new();
        self.bind_statement(&init_bind_context, stmt).await
    }

    #[async_recursion::async_recursion]
    async fn bind_statement(
        &mut self,
        bind_context: &BindContext,
        stmt: &Statement<'a>,
    ) -> Result<Plan> {
        let plan = match stmt {
            Statement::Query(query) => {
                let (s_expr, bind_context) = self.bind_query(bind_context, query).await?;
                Plan::Query {
                    s_expr: Box::new(s_expr),
                    metadata: self.metadata.clone(),
                    bind_context: Box::new(bind_context),
                    rewrite_kind: None,
                }
            }

            Statement::Explain { query, kind } => Plan::Explain {
                kind: kind.clone(),
                plan: Box::new(self.bind_statement(bind_context, query).await?),
            },

            Statement::ShowFunctions { limit } => {
                self.bind_show_functions(bind_context, limit).await?
            }

            Statement::Copy(stmt) => self.bind_copy(bind_context, stmt).await?,

            Statement::ShowMetrics => {
                self.bind_rewrite_to_query(
                    bind_context,
                    "SELECT metric, kind, labels, value FROM system.metrics",
                    RewriteKind::ShowMetrics
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
            },
            Statement::ShowSettings { like } => self.bind_show_settings(bind_context, like).await?,

            // Databases
            Statement::ShowDatabases(stmt) => self.bind_show_databases(bind_context, stmt).await?,
            Statement::ShowCreateDatabase(stmt) => self.bind_show_create_database(stmt).await?,
            Statement::CreateDatabase(stmt) => self.bind_create_database(stmt).await?,
            Statement::DropDatabase(stmt) => self.bind_drop_database(stmt).await?,
            Statement::UndropDatabase(stmt) => self.bind_undrop_database(stmt).await?,
            Statement::AlterDatabase(stmt) => self.bind_alter_database(stmt).await?,
            Statement::UseDatabase { database } =>  {
                Plan::UseDatabase(Box::new(UseDatabasePlan {
                    database: database.name.clone(),
                }))
            }
            // Tables
            Statement::ShowTables(stmt) => self.bind_show_tables(bind_context, stmt).await?,
            Statement::ShowCreateTable(stmt) => self.bind_show_create_table(stmt).await?,
            Statement::DescribeTable(stmt) => self.bind_describe_table(stmt).await?,
            Statement::ShowTablesStatus(stmt) => {
                self.bind_show_tables_status(bind_context, stmt).await?
            }
            Statement::CreateTable(stmt) => self.bind_create_table(stmt).await?,
            Statement::DropTable(stmt) => self.bind_drop_table(stmt).await?,
            Statement::UndropTable(stmt) => self.bind_undrop_table(stmt).await?,
            Statement::AlterTable(stmt) => self.bind_alter_table(stmt).await?,
            Statement::RenameTable(stmt) => self.bind_rename_table(stmt).await?,
            Statement::TruncateTable(stmt) => self.bind_truncate_table(stmt).await?,
            Statement::OptimizeTable(stmt) => self.bind_optimize_table(stmt).await?,
            Statement::ExistsTable(stmt) => self.bind_exists_table(stmt).await?,

            // Views
            Statement::CreateView(stmt) => self.bind_create_view(stmt).await?,
            Statement::AlterView(stmt) => self.bind_alter_view(stmt).await?,
            Statement::DropView(stmt) => self.bind_drop_view(stmt).await?,

            // Users
            Statement::CreateUser(stmt) => self.bind_create_user(stmt).await?,
            Statement::DropUser { if_exists, user } => Plan::DropUser(Box::new(DropUserPlan {
                if_exists: *if_exists,
                user: user.clone(),
            })),
            Statement::ShowUsers => self.bind_rewrite_to_query(bind_context, "SELECT name, hostname, auth_type, auth_string FROM system.users ORDER BY name",  RewriteKind::ShowUsers).await?,
            Statement::AlterUser(stmt) => self.bind_alter_user(stmt).await?,

            // Roles
            Statement::ShowRoles => self.bind_rewrite_to_query(bind_context, "SELECT name, inherited_roles FROM system.roles ORDER BY name", RewriteKind::ShowRoles).await?,
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
                self.bind_list_stage(location, pattern).await?
            }
            Statement::DescribeStage { stage_name } => self.bind_rewrite_to_query(bind_context, format!("SELECT * FROM system.stages WHERE name = '{stage_name}'").as_str(), RewriteKind::DescribeStage).await?,
            Statement::CreateStage(stmt) => self.bind_create_stage(stmt).await?,
            Statement::DropStage {
                stage_name,
                if_exists,
            } => Plan::DropStage(Box::new(DropUserStagePlan {
                if_exists: *if_exists,
                name: stage_name.clone(),
            })),
            Statement::RemoveStage { location, pattern } => {
                self.bind_remove_stage(location, pattern).await?
            }
            Statement::Insert(stmt) => self.bind_insert(bind_context, stmt).await?,
            Statement::Delete {
                table_reference,
                selection,
            } => {
                self.bind_delete(bind_context, table_reference, selection)
                    .await?
            }

            // Permissions
            Statement::Grant(stmt) => self.bind_grant(stmt).await?,
            Statement::ShowGrants { principal } => Plan::ShowGrants(Box::new(ShowGrantsPlan {
                principal: principal.clone(),
            })),
            Statement::Revoke(stmt) => self.bind_revoke(stmt).await?,

            // UDFs
            Statement::CreateUDF {
                if_not_exists,
                udf_name,
                parameters,
                definition,
                description,
            } => {
                let mut validator = UDFValidator {
                    name : udf_name.to_string(),
                    parameters: parameters.iter().map(|v| v.to_string()).collect(),
                    ..Default::default()
                };
                validator.verify_definition_expr(definition)?;
                let udf =  UserDefinedFunction {
                    name: validator.name,
                    parameters: validator.parameters,
                    definition: definition.to_string(),
                    description: description.clone().unwrap_or_default(),
                };

                Plan::CreateUDF(Box::new(CreateUserUDFPlan {
                    if_not_exists: *if_not_exists,
                    udf
                }))
            },
            Statement::AlterUDF {
                udf_name,
                parameters,
                definition,
                description,
            } => {
                let mut validator = UDFValidator {
                    name : udf_name.to_string(),
                    parameters: parameters.iter().map(|v| v.to_string()).collect(),
                    ..Default::default()
                };
                validator.verify_definition_expr(definition)?;
                let udf =  UserDefinedFunction {
                    name: validator.name,
                    parameters: validator.parameters,
                    definition: definition.to_string(),
                    description: description.clone().unwrap_or_default(),
                };

                Plan::AlterUDF(Box::new(AlterUserUDFPlan {
                    udf,
                }))
            }
            Statement::DropUDF {
                if_exists,
                udf_name,
            } => Plan::DropUDF(Box::new(DropUserUDFPlan {
                if_exists: *if_exists,
                name: udf_name.to_string(),
            })),
            Statement::Call(stmt) => Plan::Call(Box::new(CallPlan {
                name: stmt.name.clone(),
                args: stmt.args.clone(),
            })),

            Statement::Presign(stmt) => self.bind_presign(bind_context, stmt).await?,

            Statement::SetVariable {
                is_global,
                variable,
                value,
            } => {
                self.bind_set_variable(bind_context, *is_global, variable, value)
                    .await?
            }
            Statement::KillStmt { kill_target, object_id } => {
                self.bind_kill_stmt(bind_context, kill_target, object_id.as_str())
                    .await?
            }

            // share statements
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
        };
        Ok(plan)
    }

    async fn bind_rewrite_to_query(
        &mut self,
        bind_context: &BindContext,
        query: &str,
        rewrite_kind_r: RewriteKind,
    ) -> Result<Plan> {
        let tokens = tokenize_sql(query)?;
        let backtrace = Backtrace::new();
        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
        let mut plan = self.bind_statement(bind_context, &stmt).await?;

        if let Plan::Query { rewrite_kind, .. } = &mut plan {
            *rewrite_kind = Some(rewrite_kind_r)
        }
        Ok(plan)
    }

    /// Create a new ColumnBinding with assigned index
    pub(super) fn create_column_binding(
        &mut self,
        database_name: Option<String>,
        table_name: Option<String>,
        column_name: String,
        data_type: DataTypeImpl,
    ) -> ColumnBinding {
        let index =
            self.metadata
                .write()
                .add_column(column_name.clone(), data_type.clone(), None, None);
        ColumnBinding {
            database_name,
            table_name,
            column_name,
            index,
            data_type: Box::new(data_type),
            visible_in_unqualified_wildcard: true,
        }
    }
}
