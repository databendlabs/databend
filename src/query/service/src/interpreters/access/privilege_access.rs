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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::UserGrantSet;
use common_meta_app::principal::UserPrivilegeType;
use common_sql::plans::CopyPlan;
use common_sql::plans::RewriteKind;
use common_users::RoleCacheManager;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
}

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess { ctx })
    }
}

#[async_trait::async_trait]
impl AccessChecker for PrivilegeAccess {
    #[async_backtrace::framed]
    async fn check(&self, plan: &Plan) -> Result<()> {
        let session = self.ctx.get_current_session();
        let user = self.ctx.get_current_user()?;
        let (identity, grant_set) = (user.identity().to_string(), user.grants);
        let tenant = self.ctx.get_tenant();

        match plan {
            Plan::Query {
                metadata,
                rewrite_kind,
                ..
            } => {
                match rewrite_kind {
                    Some(RewriteKind::ShowDatabases)
                    | Some(RewriteKind::ShowEngines)
                    | Some(RewriteKind::ShowFunctions)
                    | Some(RewriteKind::ShowTableFunctions) => {
                        return Ok(());
                    }
                    Some(RewriteKind::ShowTables(database)) => {
                        let has_priv = has_priv(&tenant, database, None, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied, user {} don't have privilege for database {}",
                                identity, database
                            )))
                        };
                    }
                    Some(RewriteKind::ShowColumns(database, table)) => {
                        let has_priv = has_priv(&tenant, database, Some(table), grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied, user {} don't have privilege for table {}.{}",
                                identity, database, table
                            )))
                        };
                    }
                    _ => {}
                };
                let metadata = metadata.read().clone();
                for table in metadata.tables() {
                    if table.is_source_of_view() {
                        continue;
                    }
                    session
                        .validate_privilege(
                            &GrantObject::Table(
                                table.catalog().to_string(),
                                table.database().to_string(),
                                table.name().to_string(),
                            ),
                            vec![UserPrivilegeType::Select],
                        )
                        .await?
                }
            }
            Plan::ExplainAnalyze { plan } | Plan::Explain { plan, .. } => self.check(plan).await?,

            // Database.
            Plan::ShowCreateDatabase(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Select],
                    )
                    .await?
            }
            Plan::CreateUDF(_) | Plan::CreateDatabase(_) | Plan::CreateIndex(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Create])
                    .await?;
            }
            Plan::DropDatabase(_)
            | Plan::UndropDatabase(_)
            | Plan::DropUDF(_)
            | Plan::DropIndex(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Drop])
                    .await?;
            }
            Plan::UseDatabase(plan) => {
                // Use db is special. Should not check the privilege.
                // Just need to check user grant objects contain the db that be used.
                let database = &plan.database;
                let has_priv = has_priv(&tenant, database, None, grant_set).await?;

                return if has_priv {
                    Ok(())
                } else {
                    Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied, user {} don't have privilege for database {}",
                        identity, database
                    )))
                };
            }

            // Virtual Column.
            Plan::CreateVirtualColumns(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Create],
                    )
                    .await?;
            }
            Plan::AlterVirtualColumns(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::DropVirtualColumns(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Drop],
                    )
                    .await?;
            }
            Plan::GenerateVirtualColumns(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Super],
                    )
                    .await?;
            }

            // Table.
            Plan::ShowCreateTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Select],
                    )
                    .await?
            }
            Plan::DescribeTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Select],
                    )
                    .await?
            }
            Plan::CreateTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Create],
                    )
                    .await?;
            }
            Plan::DropTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Drop],
                    )
                    .await?;
            }
            Plan::UndropTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Drop],
                    )
                    .await?;
            }
            Plan::RenameTable(plan) => {
                // You must have ALTER and DROP privileges for the original table,
                // and CREATE and INSERT privileges for the new table.
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter, UserPrivilegeType::Drop],
                    )
                    .await?;
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.new_database.clone(),
                            plan.new_table.clone(),
                        ),
                        vec![UserPrivilegeType::Create, UserPrivilegeType::Insert],
                    )
                    .await?;
            }
            Plan::SetOptions(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::AddTableColumn(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::RenameTableColumn(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::ModifyTableColumn(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::DropTableColumn(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::AlterTableClusterKey(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::DropTableClusterKey(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Drop],
                    )
                    .await?;
            }
            Plan::ReclusterTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::TruncateTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Delete],
                    )
                    .await?;
            }
            Plan::OptimizeTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Super],
                    )
                    .await?;
            }
            Plan::VacuumTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Super],
                    )
                    .await?;
            }
            Plan::VacuumDropTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Super],
                    )
                    .await?;
            }
            Plan::AnalyzeTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Super],
                    )
                    .await?;
            }
            // Others.
            Plan::Insert(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Insert],
                    )
                    .await?;
            }
            Plan::Replace(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete],
                    )
                    .await?;
            }
            Plan::Delete(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog_name.clone(),
                            plan.database_name.clone(),
                            plan.table_name.clone(),
                        ),
                        vec![UserPrivilegeType::Delete],
                    )
                    .await?;
            }
            Plan::Update(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        vec![UserPrivilegeType::Update],
                    )
                    .await?;
            }
            Plan::CreateView(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Create],
                    )
                    .await?;
            }
            Plan::AlterView(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Alter],
                    )
                    .await?;
            }
            Plan::DropView(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        vec![UserPrivilegeType::Drop],
                    )
                    .await?;
            }
            Plan::CreateUser(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::CreateUser])
                    .await?;
            }
            Plan::DropUser(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::DropUser])
                    .await?;
            }
            Plan::CreateRole(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::CreateRole])
                    .await?;
            }
            Plan::DropRole(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::DropRole])
                    .await?;
            }
            Plan::GrantShareObject(_)
            | Plan::RevokeShareObject(_)
            | Plan::AlterShareTenants(_)
            | Plan::ShowObjectGrantPrivileges(_)
            | Plan::ShowGrantTenantsOfShare(_)
            | Plan::ShowGrants(_)
            | Plan::ShowRoles(_)
            | Plan::GrantRole(_)
            | Plan::GrantPriv(_)
            | Plan::RevokePriv(_)
            | Plan::RevokeRole(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Grant])
                    .await?;
            }
            Plan::SetVariable(_) | Plan::UnSetVariable(_) | Plan::Kill(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Super])
                    .await?;
            }
            Plan::AlterUser(_)
            | Plan::AlterUDF(_)
            | Plan::RenameDatabase(_)
            | Plan::RevertTable(_)
            | Plan::RefreshIndex(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Alter])
                    .await?;
            }
            Plan::Copy(plan) => match plan.as_ref() {
                CopyPlan::IntoTable(plan) => {
                    session
                        .validate_privilege(
                            &GrantObject::Table(
                                plan.catalog_info.catalog_name().to_string(),
                                plan.database_name.to_string(),
                                plan.table_name.to_string(),
                            ),
                            vec![UserPrivilegeType::Insert],
                        )
                        .await?;
                }
                CopyPlan::IntoStage { .. } => {
                    session
                        .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Super])
                        .await?;
                }
                CopyPlan::NoFileToCopy => {}
            },
            Plan::CreateShareEndpoint(_)
            | Plan::ShowShareEndpoint(_)
            | Plan::DropShareEndpoint(_)
            | Plan::CreateShare(_)
            | Plan::DropShare(_)
            | Plan::DescShare(_)
            | Plan::ShowShares(_)
            | Plan::Call(_)
            | Plan::ShowCreateCatalog(_)
            | Plan::CreateCatalog(_)
            | Plan::DropCatalog(_)
            | Plan::CreateStage(_)
            | Plan::DropStage(_)
            | Plan::RemoveStage(_)
            | Plan::CreateFileFormat(_)
            | Plan::DropFileFormat(_)
            | Plan::ShowFileFormats(_)
            | Plan::CreateNetworkPolicy(_)
            | Plan::AlterNetworkPolicy(_)
            | Plan::DropNetworkPolicy(_)
            | Plan::DescNetworkPolicy(_)
            | Plan::ShowNetworkPolicies(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![UserPrivilegeType::Super])
                    .await?;
            }
            Plan::CreateDatamaskPolicy(_) | Plan::DropDatamaskPolicy(_) => {
                session
                    .validate_privilege(&GrantObject::Global, vec![
                        UserPrivilegeType::CreateDataMask,
                    ])
                    .await?;
            }
            // Note: No need to check privileges
            // SET ROLE is a session-local statement (have same semantic with the SET ROLE in postgres), no need to check privileges
            Plan::SetRole(_) => {}
            Plan::Presign(_) => {}
            Plan::ExplainAst { .. } => {}
            Plan::ExplainSyntax { .. } => {}
            // just used in clickhouse-sqlalchemy, no need to check
            Plan::ExistsTable(_) => {}
            Plan::DescDatamaskPolicy(_) => {}
        }

        Ok(())
    }
}

async fn has_priv(
    tenant: &str,
    database: &String,
    table: Option<&String>,
    grant_set: UserGrantSet,
) -> Result<bool> {
    Ok(RoleCacheManager::instance()
        .find_related_roles(tenant, &grant_set.roles())
        .await?
        .into_iter()
        .map(|role| role.grants)
        .fold(grant_set, |a, b| a | b)
        .entries()
        .iter()
        .any(|e| {
            let object = e.object();
            match object {
                GrantObject::Global => true,
                GrantObject::Database(_, ldb) => ldb == database,
                GrantObject::Table(_, ldb, ltab) => {
                    if let Some(table) = table {
                        ldb == database && ltab == table
                    } else {
                        ldb == database
                    }
                }
            }
        }))
}
