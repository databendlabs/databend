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

use databend_common_catalog::database::Database;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_sql::plans::AlterSharePlan;
use databend_common_sql::plans::AlterSharePlanAction;
use databend_common_sql::plans::CreateDatabaseFromSharePlan;
use databend_common_sql::plans::CreateSharePlan;
use databend_common_sql::plans::DescSharePlan;
use databend_common_sql::plans::DropSharePlan;
use databend_common_sql::plans::GrantSharePlan;
use databend_common_sql::plans::RevokeSharePlan;
use databend_common_sql::plans::ShareGrantObject;
use databend_common_sql::plans::ShareGrantObjectPrivilege;
use databend_common_sql::plans::ShowSharesPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_meta_client::types::MatchSeq;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextTableAccess;
use crate::share::SHARE_ENGINE;
use crate::share::ShareGrantDatabase;
use crate::share::ShareGrantTable;
use crate::share::share_service;

pub struct CreateShareInterpreter {
    plan: CreateSharePlan,
}

impl CreateShareInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: CreateSharePlan) -> Result<Self> {
        Ok(Self { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateShareInterpreter {
    fn name(&self) -> &str {
        "CreateShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        share_service()
            .create_share(
                &self.plan.tenant,
                self.plan.create_option,
                &self.plan.name,
                self.plan.comment.clone(),
            )
            .await?;
        Ok(PipelineBuildResult::create())
    }
}

pub struct DropShareInterpreter {
    plan: DropSharePlan,
}

impl DropShareInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: DropSharePlan) -> Result<Self> {
        Ok(Self { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropShareInterpreter {
    fn name(&self) -> &str {
        "DropShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        share_service()
            .drop_share(&self.plan.tenant, &self.plan.name)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}

pub struct AlterShareInterpreter {
    plan: AlterSharePlan,
}

impl AlterShareInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: AlterSharePlan) -> Result<Self> {
        Ok(Self { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterShareInterpreter {
    fn name(&self) -> &str {
        "AlterShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let service = share_service();
        match &self.plan.action {
            AlterSharePlanAction::AddAccounts { accounts } => {
                service
                    .add_accounts(
                        &self.plan.tenant,
                        &self.plan.name,
                        accounts.clone(),
                        self.plan.if_exists,
                    )
                    .await?;
            }
            AlterSharePlanAction::RemoveAccounts { accounts } => {
                service
                    .remove_accounts(
                        &self.plan.tenant,
                        &self.plan.name,
                        accounts.clone(),
                        self.plan.if_exists,
                    )
                    .await?;
            }
            AlterSharePlanAction::Set { accounts, comment } => {
                service
                    .set_share(
                        &self.plan.tenant,
                        &self.plan.name,
                        accounts.clone(),
                        comment.clone(),
                        self.plan.if_exists,
                    )
                    .await?;
            }
        }
        Ok(PipelineBuildResult::create())
    }
}

pub struct GrantShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: GrantSharePlan,
}

impl GrantShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GrantSharePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for GrantShareInterpreter {
    fn name(&self) -> &str {
        "GrantShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        match (&self.plan.privilege, &self.plan.object) {
            (ShareGrantObjectPrivilege::Usage, ShareGrantObject::Database { database }) => {
                let db = self.provider_database(database).await?;
                self.ensure_database_can_be_shared(db.as_ref())?;
                share_service()
                    .grant_database(&self.plan.tenant, &self.plan.share, ShareGrantDatabase {
                        database: database.clone(),
                        database_id: db.get_db_info().database_id.db_id,
                    })
                    .await?;
            }
            (ShareGrantObjectPrivilege::Select, ShareGrantObject::Table { database, table }) => {
                let database = self.resolve_table_database(database.as_deref())?;
                let db = self.provider_database(&database).await?;
                self.ensure_database_can_be_shared(db.as_ref())?;
                let table_ref = db.get_table(table).await?;
                let storage_params = table_ref
                    .get_table_info()
                    .meta
                    .storage_params
                    .clone()
                    .unwrap_or_else(|| GlobalConfig::instance().storage.params.clone());
                share_service()
                    .grant_table(&self.plan.tenant, &self.plan.share, ShareGrantTable {
                        database,
                        database_id: db.get_db_info().database_id.db_id,
                        table: table.clone(),
                        table_id: table_ref.get_id(),
                        storage_params,
                    })
                    .await?;
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "Only USAGE ON DATABASE and SELECT ON TABLE can be granted to a share",
                ));
            }
        }

        Ok(PipelineBuildResult::create())
    }
}

impl GrantShareInterpreter {
    async fn provider_database(&self, database: &str) -> Result<Arc<dyn Database>> {
        let catalog = self.ctx.get_default_catalog()?;
        catalog.get_database(&self.plan.tenant, database).await
    }

    fn ensure_database_can_be_shared(&self, db: &dyn Database) -> Result<()> {
        if db.engine().eq_ignore_ascii_case(SHARE_ENGINE) {
            return Err(ErrorCode::InvalidOperation(
                "Cannot grant a shared database to a share",
            ));
        }
        Ok(())
    }

    fn resolve_table_database(&self, database: Option<&str>) -> Result<String> {
        match database {
            Some(database) => Ok(database.to_string()),
            None => {
                let database = self.ctx.get_current_database();
                if database.is_empty() {
                    Err(ErrorCode::UnknownDatabase("No database selected"))
                } else {
                    Ok(database)
                }
            }
        }
    }
}

pub struct RevokeShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: RevokeSharePlan,
}

impl RevokeShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RevokeSharePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RevokeShareInterpreter {
    fn name(&self) -> &str {
        "RevokeShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        match (&self.plan.privilege, &self.plan.object) {
            (ShareGrantObjectPrivilege::Usage, ShareGrantObject::Database { database }) => {
                share_service()
                    .revoke_database(&self.plan.tenant, &self.plan.share, database)
                    .await?;
            }
            (ShareGrantObjectPrivilege::Select, ShareGrantObject::Table { database, table }) => {
                let database = match database {
                    Some(database) => database.clone(),
                    None => self.ctx.get_current_database(),
                };
                share_service()
                    .revoke_table(&self.plan.tenant, &self.plan.share, &database, table)
                    .await?;
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "Only USAGE ON DATABASE and SELECT ON TABLE can be revoked from a share",
                ));
            }
        }

        Ok(PipelineBuildResult::create())
    }
}

pub struct ShowSharesInterpreter {
    plan: ShowSharesPlan,
}

impl ShowSharesInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: ShowSharesPlan) -> Result<Self> {
        Ok(Self { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowSharesInterpreter {
    fn name(&self) -> &str {
        "ShowSharesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let rows = share_service()
            .show_shares(
                &self.plan.tenant,
                self.plan.like.as_deref(),
                self.plan.limit,
            )
            .await?;

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(
                rows.iter()
                    .map(|row| row.created_on.clone())
                    .collect::<Vec<_>>(),
            ),
            StringType::from_data(rows.iter().map(|row| row.kind.clone()).collect::<Vec<_>>()),
            StringType::from_data(
                rows.iter()
                    .map(|row| row.owner_account.clone())
                    .collect::<Vec<_>>(),
            ),
            StringType::from_data(rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>()),
            StringType::from_data(
                rows.iter()
                    .map(|row| row.database_name.clone())
                    .collect::<Vec<_>>(),
            ),
            StringType::from_data(rows.iter().map(|row| row.to.clone()).collect::<Vec<_>>()),
            StringType::from_data(rows.iter().map(|row| row.owner.clone()).collect::<Vec<_>>()),
            StringType::from_data(
                rows.iter()
                    .map(|row| row.comment.clone())
                    .collect::<Vec<_>>(),
            ),
            StringType::from_data(
                rows.iter()
                    .map(|row| row.listing_global_name.clone())
                    .collect::<Vec<_>>(),
            ),
        ])])
    }
}

pub struct DescShareInterpreter {
    plan: DescSharePlan,
}

impl DescShareInterpreter {
    pub fn try_create(_ctx: Arc<QueryContext>, plan: DescSharePlan) -> Result<Self> {
        Ok(Self { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescShareInterpreter {
    fn name(&self) -> &str {
        "DescShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let rows = share_service()
            .describe_share(
                &self.plan.tenant,
                self.plan.provider_tenant.as_deref(),
                &self.plan.share,
            )
            .await?;

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(rows.iter().map(|row| row.kind.clone()).collect::<Vec<_>>()),
            StringType::from_data(rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>()),
            StringType::from_data(
                rows.iter()
                    .map(|row| row.shared_on.clone())
                    .collect::<Vec<_>>(),
            ),
        ])])
    }
}

pub struct CreateDatabaseFromShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateDatabaseFromSharePlan,
}

impl CreateDatabaseFromShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateDatabaseFromSharePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateDatabaseFromShareInterpreter {
    fn name(&self) -> &str {
        "CreateDatabaseFromShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let binding = share_service()
            .bind_share_database(
                &self.plan.tenant,
                &self.plan.provider_tenant,
                &self.plan.share,
            )
            .await?;

        let quota_api = UserApiProvider::instance().tenant_quota_api(&self.plan.tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let databases = catalog.list_databases(&self.plan.tenant).await?;
        if quota.max_databases != 0 && databases.len() >= quota.max_databases as usize {
            return Err(ErrorCode::TenantQuotaExceeded(format!(
                "Max databases quota exceeded {}",
                quota.max_databases
            )));
        }

        let req = CreateDatabaseReq {
            create_option: self.plan.create_option,
            catalog_name: if self.plan.create_option.is_overriding() {
                Some(self.plan.catalog.clone())
            } else {
                None
            },
            name_ident: DatabaseNameIdent::new(&self.plan.tenant, &self.plan.database),
            meta: DatabaseMeta {
                engine: SHARE_ENGINE.to_string(),
                engine_options: binding.to_engine_options(),
                ..Default::default()
            },
        };

        let reply = catalog.create_database(req).await?;

        if let Some(current_role) = self.ctx.get_current_role() {
            if !catalog.is_external() {
                let role_api = UserApiProvider::instance().role_api(&self.plan.tenant);
                role_api
                    .grant_ownership(
                        &OwnershipObject::Database {
                            catalog_name: self.plan.catalog.clone(),
                            db_id: *reply.db_id,
                        },
                        &current_role.name,
                    )
                    .await?;
                RoleCacheManager::instance().invalidate_cache(&self.plan.tenant);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
