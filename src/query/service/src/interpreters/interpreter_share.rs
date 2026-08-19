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

use std::collections::BTreeSet;
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
use crate::interpreters::access::PrivilegeAccess;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextTableAccess;
use crate::share::ProviderObjectIds;
use crate::share::SHARE_ENGINE;
use crate::share::SetShareRequest;
use crate::share::ShareGrantDatabase;
use crate::share::ShareGrantTable;
use crate::share::ShareMgr;
use crate::share::ensure_provider_table_can_be_shared;

fn share_mgr() -> ShareMgr {
    ShareMgr::create(UserApiProvider::instance().get_meta_store_client())
}

pub struct CreateShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateSharePlan,
}

impl CreateShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateSharePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
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
        let service = share_mgr();
        if service.exists(&self.plan.tenant, &self.plan.name).await? {
            match self.plan.create_option {
                databend_common_meta_app::schema::CreateOption::CreateIfNotExists => {
                    return Ok(PipelineBuildResult::create());
                }
                databend_common_meta_app::schema::CreateOption::Create => {
                    service
                        .create_share(
                            &self.plan.tenant,
                            self.plan.create_option,
                            &self.plan.name,
                            self.plan.connection.clone(),
                            self.plan.comment.clone(),
                        )
                        .await?;
                    return Ok(PipelineBuildResult::create());
                }
                databend_common_meta_app::schema::CreateOption::CreateOrReplace => {}
            }
        }
        if let Some(connection) = &self.plan.connection {
            PrivilegeAccess::validate_share_management_for_connection(
                self.ctx.clone(),
                Some(connection),
            )
            .await?;
            UserApiProvider::instance()
                .get_connection(&self.plan.tenant, connection)
                .await?;
        }
        service
            .create_share(
                &self.plan.tenant,
                self.plan.create_option,
                &self.plan.name,
                self.plan.connection.clone(),
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
        let service = share_mgr();
        if self.plan.if_exists && !service.exists(&self.plan.tenant, &self.plan.name).await? {
            return Ok(PipelineBuildResult::create());
        }
        service
            .drop_share(&self.plan.tenant, &self.plan.name)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}

pub struct AlterShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterSharePlan,
}

impl AlterShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterSharePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }

    async fn validate_replacement_connection(
        &self,
        service: &ShareMgr,
        connection: &str,
    ) -> Result<BTreeSet<u64>> {
        PrivilegeAccess::validate_share_management_for_connection(
            self.ctx.clone(),
            Some(connection),
        )
        .await?;
        UserApiProvider::instance()
            .get_connection(&self.plan.tenant, connection)
            .await?;
        let table_ids = service
            .get_granted_table_ids(&self.plan.tenant, &self.plan.name)
            .await?;
        let catalog = self.ctx.get_default_catalog()?;
        for table_id in &table_ids {
            let table_meta = catalog
                .get_table_meta_by_id(*table_id)
                .await?
                .ok_or_else(|| {
                    ErrorCode::InvalidOperation(format!(
                        "Cannot validate replacement connection: granted provider table id {} no longer exists",
                        table_id
                    ))
                })?;
            ensure_provider_table_can_be_shared(&table_meta.data)?;
            let storage_params = table_meta
                .data
                .storage_params
                .clone()
                .unwrap_or_else(|| GlobalConfig::instance().storage.params.clone());
            crate::share::resolve_share_storage_params(
                &self.plan.tenant,
                connection,
                storage_params,
            )
            .await?;
        }
        Ok(table_ids)
    }

    async fn authorize_required_current_connection(&self, service: &ShareMgr) -> Result<String> {
        let connection = service
            .get_connection_name(&self.plan.tenant, &self.plan.name)
            .await?;
        PrivilegeAccess::validate_share_management_for_connection(
            self.ctx.clone(),
            Some(&connection),
        )
        .await?;
        Ok(connection)
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
        let service = share_mgr();
        if self.plan.if_exists && !service.exists(&self.plan.tenant, &self.plan.name).await? {
            return Ok(PipelineBuildResult::create());
        }
        match &self.plan.action {
            AlterSharePlanAction::AddAccounts { accounts } => {
                let expected_connection =
                    self.authorize_required_current_connection(&service).await?;
                service
                    .add_accounts(
                        &self.plan.tenant,
                        &self.plan.name,
                        accounts.clone(),
                        expected_connection,
                        self.plan.if_exists,
                    )
                    .await?;
            }
            AlterSharePlanAction::RemoveAccounts { accounts } => {
                let expected_connection = service
                    .get_connection_name_if_exists(&self.plan.tenant, &self.plan.name)
                    .await?;
                service
                    .remove_accounts(
                        &self.plan.tenant,
                        &self.plan.name,
                        accounts.clone(),
                        expected_connection,
                        self.plan.if_exists,
                    )
                    .await?;
            }
            AlterSharePlanAction::Set {
                accounts,
                connection,
                comment,
            } => {
                let request = match (accounts, connection) {
                    (accounts, Some(connection)) => SetShareRequest::connection(
                        accounts.clone(),
                        comment.clone(),
                        connection.clone(),
                        self.validate_replacement_connection(&service, connection)
                            .await?,
                        self.plan.if_exists,
                    ),
                    (Some(accounts), None) => SetShareRequest::accounts(
                        accounts.clone(),
                        comment.clone(),
                        self.authorize_required_current_connection(&service).await?,
                        self.plan.if_exists,
                    ),
                    (None, None) => {
                        SetShareRequest::properties(comment.clone(), self.plan.if_exists)
                    }
                };
                service
                    .set_share(&self.plan.tenant, &self.plan.name, request)
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
                let db = provider_database(&self.ctx, &self.plan.tenant, database).await?;
                ensure_database_can_be_shared(db.as_ref())?;
                let grant = ShareGrantDatabase {
                    database: database.clone(),
                    database_id: db.get_db_info().database_id.db_id,
                    database_meta_seq: db.get_db_info().meta.seq,
                };
                PrivilegeAccess::validate_share_database_grant(self.ctx.clone(), &grant).await?;
                share_mgr()
                    .grant_database(&self.plan.tenant, &self.plan.share, grant)
                    .await?;
            }
            (ShareGrantObjectPrivilege::Select, ShareGrantObject::Table { database, table }) => {
                let database = self.resolve_table_database(database.as_deref())?;
                let db = provider_database(&self.ctx, &self.plan.tenant, &database).await?;
                ensure_database_can_be_shared(db.as_ref())?;
                let table_ref = db.get_table(table).await?;
                ensure_provider_table_can_be_shared(&table_ref.get_table_info().meta)?;
                let storage_params = table_ref
                    .get_table_info()
                    .meta
                    .storage_params
                    .clone()
                    .unwrap_or_else(|| GlobalConfig::instance().storage.params.clone());
                let grant = ShareGrantTable {
                    database,
                    database_id: db.get_db_info().database_id.db_id,
                    database_meta_seq: db.get_db_info().meta.seq,
                    table: table.clone(),
                    table_id: table_ref.get_id(),
                    table_meta_seq: table_ref.get_table_info().ident.seq,
                };
                PrivilegeAccess::validate_share_table_grant(self.ctx.clone(), &grant).await?;
                let manager = share_mgr();
                let connection = manager
                    .get_connection_name(&self.plan.tenant, &self.plan.share)
                    .await?;
                PrivilegeAccess::validate_share_management_for_connection(
                    self.ctx.clone(),
                    Some(&connection),
                )
                .await?;
                crate::share::resolve_share_storage_params(
                    &self.plan.tenant,
                    &connection,
                    storage_params,
                )
                .await?;
                manager
                    .grant_table(&self.plan.tenant, &self.plan.share, grant, connection)
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
    fn resolve_table_database(&self, database: Option<&str>) -> Result<String> {
        resolve_table_database(&self.ctx, database)
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
        let manager = share_mgr();
        match (&self.plan.privilege, &self.plan.object) {
            (ShareGrantObjectPrivilege::Usage, ShareGrantObject::Database { database }) => {
                let current_database_id =
                    provider_database_id_if_exists(&self.ctx, &self.plan.tenant, database).await?;
                if let Some(target) = manager
                    .prepare_revoke_database(
                        &self.plan.tenant,
                        &self.plan.share,
                        current_database_id,
                    )
                    .await?
                {
                    PrivilegeAccess::validate_share_revoke_target(self.ctx.clone(), &target)
                        .await?;
                    manager
                        .revoke_share_object(&self.plan.tenant, &self.plan.share, target)
                        .await?;
                }
            }
            (ShareGrantObjectPrivilege::Select, ShareGrantObject::Table { database, table }) => {
                let database = resolve_table_database(&self.ctx, database.as_deref())?;
                let current_object_ids =
                    provider_table_id_if_exists(&self.ctx, &self.plan.tenant, &database, table)
                        .await?;
                if let Some(target) = manager
                    .prepare_revoke_table(&self.plan.tenant, &self.plan.share, current_object_ids)
                    .await?
                {
                    PrivilegeAccess::validate_share_revoke_target(self.ctx.clone(), &target)
                        .await?;
                    manager
                        .revoke_share_object(&self.plan.tenant, &self.plan.share, target)
                        .await?;
                }
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

async fn provider_database(
    ctx: &QueryContext,
    tenant: &databend_common_meta_app::tenant::Tenant,
    database: &str,
) -> Result<Arc<dyn Database>> {
    let catalog = ctx.get_default_catalog()?;
    catalog.get_database(tenant, database).await
}

async fn provider_database_id_if_exists(
    ctx: &QueryContext,
    tenant: &databend_common_meta_app::tenant::Tenant,
    database: &str,
) -> Result<Option<u64>> {
    match provider_database(ctx, tenant, database).await {
        Ok(database) => Ok(Some(database.get_db_info().database_id.db_id)),
        Err(err) if err.code() == ErrorCode::UNKNOWN_DATABASE => Ok(None),
        Err(err) => Err(err),
    }
}

async fn provider_table_id_if_exists(
    ctx: &QueryContext,
    tenant: &databend_common_meta_app::tenant::Tenant,
    database: &str,
    table: &str,
) -> Result<Option<ProviderObjectIds>> {
    let database = match provider_database(ctx, tenant, database).await {
        Ok(database) => database,
        Err(err) if err.code() == ErrorCode::UNKNOWN_DATABASE => return Ok(None),
        Err(err) => return Err(err),
    };
    let database_id = database.get_db_info().database_id.db_id;
    match database.get_table(table).await {
        Ok(table) => Ok(Some(ProviderObjectIds {
            database_id,
            table_id: table.get_id(),
        })),
        Err(err) if err.code() == ErrorCode::UNKNOWN_TABLE => Ok(None),
        Err(err) => Err(err),
    }
}

fn ensure_database_can_be_shared(db: &dyn Database) -> Result<()> {
    if db.engine().eq_ignore_ascii_case(SHARE_ENGINE) {
        return Err(ErrorCode::InvalidOperation(
            "Cannot grant a shared database to a share",
        ));
    }
    Ok(())
}

fn resolve_table_database(ctx: &QueryContext, database: Option<&str>) -> Result<String> {
    match database {
        Some(database) => Ok(database.to_string()),
        None => {
            let database = ctx.get_current_database();
            if database.is_empty() {
                Err(ErrorCode::UnknownDatabase("No database selected"))
            } else {
                Ok(database)
            }
        }
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
        let rows = share_mgr()
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
        let rows = share_mgr()
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
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        if catalog.is_external() {
            return Err(ErrorCode::InvalidOperation(
                "CREATE DATABASE ... FROM SHARE is only supported in the default catalog",
            ));
        }

        if self.plan.create_option.if_not_exist()
            && catalog
                .exists_database(&self.plan.tenant, &self.plan.database)
                .await?
        {
            return Ok(PipelineBuildResult::create());
        }

        let binding = share_mgr()
            .bind_share_database(
                &self.plan.tenant,
                &self.plan.provider_tenant,
                &self.plan.share,
            )
            .await?;

        let quota_api = UserApiProvider::instance().tenant_quota_api(&self.plan.tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let databases = catalog.list_databases(&self.plan.tenant).await?;
        if quota.max_databases != 0 && databases.len() >= quota.max_databases as usize {
            return Err(ErrorCode::TenantQuotaExceeded(format!(
                "Max databases quota exceeded {}",
                quota.max_databases
            )));
        }

        let req = CreateDatabaseReq {
            override_existing: self.plan.create_option.is_overriding(),
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
