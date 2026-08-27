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
use databend_common_sql::plans::GrantSharePlan;
use databend_common_sql::plans::RevokeSharePlan;
use databend_common_sql::plans::ShareGrantObject;
use databend_common_sql::plans::ShareGrantObjectPrivilege;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::interpreters::access::validate_share_management_for_connection;
use crate::interpreters::access::validate_share_object_by_id;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::share::ProviderObjectIds;
use crate::share::SHARE_ENGINE;
use crate::share::ShareGrantDatabase;
use crate::share::ShareGrantTable;
use crate::share::ShareMgr;
use crate::share::ShareRevokeTarget;
use crate::share::ensure_provider_table_can_be_shared;
use crate::share::resolve_share_storage_params;

fn share_mgr() -> ShareMgr {
    ShareMgr::create(UserApiProvider::instance().get_meta_store_client())
}

pub struct GrantShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: GrantSharePlan,
}

impl GrantShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: GrantSharePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }

    fn resolve_table_database(&self, database: Option<&str>) -> Result<String> {
        resolve_table_database(&self.ctx, database)
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
                validate_share_database_grant(self.ctx.clone(), &grant).await?;
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
                validate_share_table_grant(self.ctx.clone(), &grant).await?;
                let manager = share_mgr();
                let connection = manager
                    .get_connection_name(&self.plan.tenant, &self.plan.share)
                    .await?;
                validate_share_management_for_connection(self.ctx.clone(), Some(&connection))
                    .await?;
                resolve_share_storage_params(&self.plan.tenant, &connection, storage_params)
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
                    validate_share_revoke_target(self.ctx.clone(), &target).await?;
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
                    validate_share_revoke_target(self.ctx.clone(), &target).await?;
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

async fn validate_share_database_grant(
    ctx: Arc<QueryContext>,
    grant: &ShareGrantDatabase,
) -> Result<()> {
    validate_share_object_by_id(ctx, grant.database_id, None).await
}

async fn validate_share_table_grant(ctx: Arc<QueryContext>, grant: &ShareGrantTable) -> Result<()> {
    validate_share_object_by_id(ctx, grant.database_id, Some(grant.table_id)).await
}

async fn validate_share_revoke_target(
    ctx: Arc<QueryContext>,
    target: &ShareRevokeTarget,
) -> Result<()> {
    if !target.requires_object_privilege() {
        return Ok(());
    }

    validate_share_object_by_id(ctx, target.database_id(), target.table_id()).await
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
