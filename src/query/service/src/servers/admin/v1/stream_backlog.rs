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

use databend_common_catalog::session_type::SessionType;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::operations::StreamBacklog;
use databend_common_storages_stream::stream_table::StreamTable;
use fastrace::func_name;
use log::debug;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use serde::Deserialize;
use serde::Serialize;

use crate::sessions::SessionManager;
use crate::sessions::TableContext;

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamBacklogQuery {
    pub database: Option<String>,
    pub stream_name: String,
}

#[derive(Debug, Serialize)]
pub struct StreamBacklogResponse {
    rows_added: u64,
    rows_removed: u64,
    estimated_rows: u64,
}

impl From<StreamBacklog> for StreamBacklogResponse {
    fn from(value: StreamBacklog) -> Self {
        StreamBacklogResponse {
            rows_added: value.rows_added,
            rows_removed: value.rows_removed,
            estimated_rows: value.estimated_rows,
        }
    }
}

#[async_backtrace::framed]
async fn get_stream_backlog(
    tenant: &Tenant,
    params: Query<StreamBacklogQuery>,
) -> Result<StreamBacklogResponse> {
    let mut dummy_session = SessionManager::instance()
        .create_session(SessionType::Dummy)
        .await?;
    dummy_session.set_current_tenant(tenant.clone())?;
    let session = SessionManager::instance().register_session(dummy_session)?;

    let ctx: Arc<dyn TableContext> = session
        .create_query_context(&databend_common_version::BUILD_INFO)
        .await?;

    let db_name = params.database.clone().unwrap_or("default".to_string());
    let tbl = ctx
        .get_table("default", &db_name, &params.stream_name)
        .await?;
    let stream = StreamTable::try_from_table(tbl.as_ref())?;
    let backlog = stream.stream_backlog(ctx).await?;

    Ok(backlog.into())
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn stream_backlog_handler(
    Path(tenant): Path<String>,
    params: Query<StreamBacklogQuery>,
) -> poem::Result<impl IntoResponse> {
    debug!("stream_backlog: tenant: {}, params: {:?}", tenant, params);

    let tenant = Tenant::new_or_err(tenant, func_name!()).map_err(poem::error::BadRequest)?;

    let resp = get_stream_backlog(&tenant, params)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn stream_backlog_local_handler(
    params: Query<StreamBacklogQuery>,
) -> poem::Result<impl IntoResponse> {
    let tenant = &GlobalConfig::instance().query.tenant_id;
    debug!(
        "stream_backlog(local): tenant: {:?}, params: {:?}",
        tenant, params
    );

    let resp = get_stream_backlog(tenant, params)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}
