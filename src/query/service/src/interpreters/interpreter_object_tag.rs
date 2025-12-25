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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::tag_api::TagApi;
use databend_common_meta_app::schema::SetObjectTagsReq;
use databend_common_meta_app::schema::TagNameIdent;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_meta_app::schema::UnsetObjectTagsReq;
use databend_common_sql::plans::SetObjectTagsPlan;
use databend_common_sql::plans::TagSetObject;
use databend_common_sql::plans::UnsetObjectTagsPlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
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
            set_tags(&self.plan.tenant, object, tag_pairs).await?;
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
            unset_tags(&self.plan.tenant, object, tag_ids).await?;
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
    }
}

async fn resolve_database_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::DatabaseTagSetTarget,
) -> Result<Option<TaggableObject>> {
    let catalog = ctx.get_catalog(&target.catalog).await?;
    match catalog.get_database(tenant, &target.database).await {
        Ok(db) => Ok(Some(TaggableObject::Database {
            db_id: db.get_db_info().database_id.db_id,
        })),
        Err(e) if e.code() == ErrorCode::UNKNOWN_DATABASE && target.if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

async fn resolve_table_object(
    ctx: &Arc<QueryContext>,
    tenant: &databend_common_meta_app::tenant::Tenant,
    target: &databend_common_sql::plans::TableTagSetTarget,
) -> Result<Option<TaggableObject>> {
    let catalog = ctx.get_catalog(&target.catalog).await?;
    match catalog
        .get_table(tenant, &target.database, &target.table)
        .await
    {
        Ok(table) => Ok(Some(TaggableObject::Table {
            table_id: table.get_table_info().ident.table_id,
        })),
        Err(e) if e.code() == ErrorCode::UNKNOWN_TABLE && target.if_exists => Ok(None),
        Err(e) => Err(e),
    }
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
            .await?
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
            .await?
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
    match meta_client.set_object_tags(req).await? {
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
    meta_client.unset_object_tags(req).await?;
    Ok(())
}
