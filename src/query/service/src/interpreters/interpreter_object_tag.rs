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

use databend_common_catalog::database::is_builtin_database;
use databend_common_catalog::database::is_system_database;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::principal::GetProcedureReq;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::schema::SetObjectTagsReq;
use databend_common_meta_app::schema::TagNameIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_sql::plans::SetObjectTagsPlan;
use databend_common_sql::plans::TagSetObject;
use databend_common_sql::plans::UnsetObjectTagsPlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_users::UserApiProvider;
use log::info;

use crate::interpreters::Interpreter;
use crate::meta_service_error;
use crate::meta_txn_error;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct SetObjectTagsInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetObjectTagsPlan,
}

pub struct UnsetObjectTagsInterpreter {
    ctx: Arc<QueryContext>,
    plan: UnsetObjectTagsPlan,
}

impl SetObjectTagsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetObjectTagsPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

impl UnsetObjectTagsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UnsetObjectTagsPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetObjectTagsInterpreter {
    fn name(&self) -> &str {
        "SetObjectTagsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if let Some(object) =
            resolve_taggable_object(&self.ctx, &self.plan.tenant, &self.plan.object).await?
        {
            let tag_pairs = resolve_tag_assignments(&self.plan.tenant, &self.plan.tags).await?;
            set_tags(&self.plan.tenant, object.clone(), tag_pairs).await?;
            let tag_assignments: Vec<_> = self
                .plan
                .tags
                .iter()
                .map(|t| format!("{}='{}'", t.name, t.value))
                .collect();
            info!("set tags on {}: {}", object, tag_assignments.join(", "));
        }
        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for UnsetObjectTagsInterpreter {
    fn name(&self) -> &str {
        "UnsetObjectTagsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if let Some(object) =
            resolve_taggable_object(&self.ctx, &self.plan.tenant, &self.plan.object).await?
        {
            let tag_ids = resolve_tag_ids(&self.plan.tenant, &self.plan.tags).await?;
            unset_tags(&self.plan.tenant, object.clone(), tag_ids).await?;
            info!("unset tags from {}: {}", object, self.plan.tags.join(", "));
        }
        Ok(PipelineBuildResult::create())
    }
}

async fn resolve_taggable_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    object: &TagSetObject,
) -> Result<Option<TaggableObject>> {
    match object {
        TagSetObject::Database(target) => resolve_database_object(ctx, tenant, target).await,
        TagSetObject::Table(target) => resolve_table_object(ctx, tenant, target).await,
        TagSetObject::Stage(target) => resolve_stage_object(tenant, target).await,
        TagSetObject::Connection(target) => resolve_connection_object(tenant, target).await,
        TagSetObject::View(target) => resolve_view_object(ctx, tenant, target).await,
        TagSetObject::UDF(target) => resolve_udf_object(tenant, target).await,
        TagSetObject::Procedure(target) => resolve_procedure_object(tenant, target).await,
    }
}

async fn resolve_database_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::DatabaseTagSetTarget,
) -> Result<Option<TaggableObject>> {
    let catalog = ctx.get_catalog(&target.catalog).await?;

    // External catalog (e.g., Iceberg) does not support tags
    if catalog.is_external() {
        return Err(ErrorCode::Unimplemented(
            "Tags are not supported for external catalog databases",
        ));
    }

    // Built-in databases do not support tags
    if is_builtin_database(&target.database) {
        return Err(ErrorCode::Unimplemented(format!(
            "Tags are not supported for built-in database '{}'",
            target.database
        )));
    }

    match catalog.get_database(tenant, &target.database).await {
        Ok(db) => Ok(Some(TaggableObject::Database {
            db_id: db.get_db_info().database_id.db_id,
        })),
        Err(e) if e.code() == ErrorCode::UNKNOWN_DATABASE && target.if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

/// Shared resolver for objects in the table ID space (tables and views).
/// `expected_engine` constrains the engine type: `Some(VIEW_ENGINE)` for views, `None` for tables.
async fn resolve_table_id_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    catalog_name: &str,
    database: &str,
    table_name: &str,
    if_exists: bool,
    expected_engine: Option<&str>,
) -> Result<Option<TaggableObject>> {
    let kind = expected_engine.unwrap_or("table");
    let catalog = ctx.get_catalog(catalog_name).await?;

    if catalog.is_external() {
        return Err(ErrorCode::Unimplemented(format!(
            "Tags are not supported for external catalog {}s",
            kind
        )));
    }

    if is_system_database(database) {
        return Err(ErrorCode::Unimplemented(format!(
            "Tags are not supported for {}s in system database '{}'",
            kind, database
        )));
    }

    match catalog.get_table(tenant, database, table_name).await {
        Ok(table) => {
            if let Some(engine) = expected_engine {
                if table.engine() != engine {
                    return Err(ErrorCode::UnknownView(format!(
                        "'{}' is not a view",
                        table_name
                    )));
                }
            } else if table.is_temp() {
                return Err(ErrorCode::Unimplemented(
                    "Tags are not supported for temporary tables",
                ));
            }
            Ok(Some(TaggableObject::Table {
                table_id: table.get_table_info().ident.table_id,
            }))
        }
        Err(e) if e.code() == ErrorCode::UNKNOWN_TABLE && if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

async fn resolve_table_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::TableTagSetTarget,
) -> Result<Option<TaggableObject>> {
    resolve_table_id_object(
        ctx,
        tenant,
        &target.catalog,
        &target.database,
        &target.table,
        target.if_exists,
        None,
    )
    .await
}

async fn resolve_stage_object(
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::StageTagSetTarget,
) -> Result<Option<TaggableObject>> {
    match UserApiProvider::instance()
        .get_stage(tenant, &target.stage_name)
        .await
    {
        Ok(stage) => Ok(Some(TaggableObject::Stage {
            name: stage.stage_name,
        })),
        Err(e) if e.code() == ErrorCode::UNKNOWN_STAGE && target.if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

async fn resolve_connection_object(
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::ConnectionTagSetTarget,
) -> Result<Option<TaggableObject>> {
    match UserApiProvider::instance()
        .get_connection(tenant, &target.connection_name)
        .await
    {
        Ok(_) => Ok(Some(TaggableObject::Connection {
            name: target.connection_name.clone(),
        })),
        Err(e) if e.code() == ErrorCode::UNKNOWN_CONNECTION && target.if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

async fn resolve_view_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::ViewTagSetTarget,
) -> Result<Option<TaggableObject>> {
    resolve_table_id_object(
        ctx,
        tenant,
        &target.catalog,
        &target.database,
        &target.view,
        target.if_exists,
        Some(VIEW_ENGINE),
    )
    .await
}

async fn resolve_udf_object(
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::UDFTagSetTarget,
) -> Result<Option<TaggableObject>> {
    match UserApiProvider::instance()
        .get_udf(tenant, &target.udf_name)
        .await
    {
        Ok(Some(_)) => Ok(Some(TaggableObject::UDF {
            name: target.udf_name.clone(),
        })),
        Ok(None) => {
            if target.if_exists {
                Ok(None)
            } else {
                Err(ErrorCode::UnknownFunction(format!(
                    "Unknown UDF '{}'",
                    target.udf_name
                )))
            }
        }
        Err(e) => Err(e.into()),
    }
}

async fn resolve_procedure_object(
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::ProcedureTagSetTarget,
) -> Result<Option<TaggableObject>> {
    let req = GetProcedureReq::new(
        tenant.clone(),
        ProcedureIdentity::new(&target.name, &target.args),
    );
    match UserApiProvider::instance()
        .procedure_api(tenant)
        .get_procedure(&req)
        .await
    {
        Ok(Some(_)) => Ok(Some(TaggableObject::Procedure {
            name: target.name.clone(),
            args: target.args.clone(),
        })),
        Ok(None) => {
            if target.if_exists {
                Ok(None)
            } else {
                Err(ErrorCode::UnknownProcedure(format!(
                    "Unknown procedure '{}'",
                    target.name
                )))
            }
        }
        Err(e) => Err(ErrorCode::from_std_error(e)),
    }
}

async fn resolve_tag_assignments(
    tenant: &databend_common_meta_app::tenant::Tenant,
    tags: &[databend_common_sql::plans::TagSetPlanItem],
) -> Result<Vec<(u64, String)>> {
    let mut resolved = Vec::with_capacity(tags.len());
    let meta_client = UserApiProvider::instance().get_meta_store_client();
    for item in tags {
        let ident = TagNameIdent::new(tenant, &item.name);
        let tag = meta_client
            .get_tag(&ident)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| ErrorCode::UnknownTag(format!("Tag '{}' not found", item.name)))?;
        resolved.push((*tag.tag_id.data, item.value.clone()));
    }
    Ok(resolved)
}

async fn resolve_tag_ids(
    tenant: &databend_common_meta_app::tenant::Tenant,
    tags: &[String],
) -> Result<Vec<u64>> {
    let mut resolved = Vec::with_capacity(tags.len());
    let meta_client = UserApiProvider::instance().get_meta_store_client();
    for tag_name in tags {
        let ident = TagNameIdent::new(tenant, tag_name);
        let tag = meta_client
            .get_tag(&ident)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| ErrorCode::UnknownTag(format!("Tag '{}' not found", tag_name)))?;
        resolved.push(*tag.tag_id.data);
    }
    Ok(resolved)
}

async fn set_tags(
    tenant: &databend_common_meta_app::tenant::Tenant,
    object: TaggableObject,
    tag_pairs: Vec<(u64, String)>,
) -> Result<()> {
    if tag_pairs.is_empty() {
        return Ok(());
    }
    let meta_client = UserApiProvider::instance().get_meta_store_client();
    let req = SetObjectTagsReq {
        tenant: tenant.clone(),
        taggable_object: object,
        tags: tag_pairs,
    };
    match meta_client
        .set_object_tags(req)
        .await
        .map_err(meta_txn_error)?
    {
        Ok(_) => Ok(()),
        Err(e) => Err(ErrorCode::from(e)),
    }
}

async fn unset_tags(
    tenant: &databend_common_meta_app::tenant::Tenant,
    object: TaggableObject,
    tag_ids: Vec<u64>,
) -> Result<()> {
    if tag_ids.is_empty() {
        return Ok(());
    }
    let meta_client = UserApiProvider::instance().get_meta_store_client();
    let req = UnsetObjectTagsReq {
        tenant: tenant.clone(),
        taggable_object: object,
        tags: tag_ids,
    };
    meta_client
        .unset_object_tags(req)
        .await
        .map_err(meta_service_error)?;
    Ok(())
}

/// Clean up all tag references for a dropped object.
///
/// Since Stage/Connection do not support UNDROP, we must remove tag bindings
/// to prevent orphaned references.
///
/// # Concurrency Safety
///
/// The order "drop object first, then cleanup tags" is critical for concurrency safety.
/// Together with the object existence check in `set_object_tags`, this prevents orphans:
///
/// ```text
/// Timeline: ──────────────────────────────────────────────>
/// SET TAG:  [read tag_meta]              [txn commit: write tag ref]
/// DROP:              [delete object]  [get_tags] [unset tags]
/// ```
///
/// - If SET TAG commits after DROP deletes the object, the txn fails
///   (object existence check: seq >= 1 not met).
/// - If SET TAG commits before DROP, DROP's get_tags will see it and clean it up.
///
/// Both mechanisms are required:
/// - `set_object_tags` check → prevents writing tag refs after object is dropped
/// - This cleanup → ensures existing tag refs are removed after drop
///
/// # Other Design Notes
///
/// - This runs regardless of whether the object existed, so repeated
///   `DROP ... IF EXISTS` can clean up any leftover tag references
///   from a previous failed attempt.
/// - Tag associations for Stage/Connection are indexed by name (not a unique ID), so:
///   - Only this object's tags are affected; other objects are safe.
///   - If a new object with the same name is created before cleanup,
///     it may "inherit" orphaned tag associations. Users can fix this
///     via `ALTER ... UNSET TAG` or `DROP ... IF EXISTS`.
pub async fn cleanup_object_tags(
    tenant: &databend_common_meta_app::tenant::Tenant,
    object: TaggableObject,
) -> Result<()> {
    let meta_api = UserApiProvider::instance().get_meta_store_client();
    let tag_values = meta_api
        .get_object_tags(tenant, &object)
        .await
        .map_err(meta_service_error)?;
    if !tag_values.is_empty() {
        let tag_ids: Vec<u64> = tag_values.iter().map(|v| v.tag_id).collect();
        let req = UnsetObjectTagsReq {
            tenant: tenant.clone(),
            taggable_object: object,
            tags: tag_ids,
        };
        meta_api
            .unset_object_tags(req)
            .await
            .map_err(meta_service_error)?;
    }
    Ok(())
}
