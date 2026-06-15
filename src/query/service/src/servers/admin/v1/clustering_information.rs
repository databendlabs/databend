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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::table_functions::ClusteringInformationResponse;
use databend_common_storages_fuse::table_functions::get_clustering_information;
use fastrace::func_name;
use http::StatusCode;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use serde::Deserialize;

use crate::sessions::SessionManager;
use crate::sessions::TableContext;

#[derive(Debug, Deserialize)]
pub struct ClusteringInformationQuery {
    pub cluster_key: Option<String>,
    pub branch: Option<String>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn clustering_information_handler(
    Path((tenant, database, table)): Path<(String, String, String)>,
    Query(params): Query<ClusteringInformationQuery>,
) -> poem::Result<impl IntoResponse> {
    let tenant = Tenant::new_or_err(tenant, func_name!()).map_err(poem::error::BadRequest)?;
    let resp = load_clustering_information(&tenant, &database, &table, params)
        .await
        .map_err(clustering_information_error)?;
    Ok(Json(resp))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn clustering_information_local_handler(
    Path((database, table)): Path<(String, String)>,
    Query(params): Query<ClusteringInformationQuery>,
) -> poem::Result<impl IntoResponse> {
    let tenant = &GlobalConfig::instance().query.tenant_id;
    let resp = load_clustering_information(tenant, &database, &table, params)
        .await
        .map_err(clustering_information_error)?;
    Ok(Json(resp))
}

#[async_backtrace::framed]
async fn load_clustering_information(
    tenant: &Tenant,
    database: &str,
    table: &str,
    params: ClusteringInformationQuery,
) -> Result<ClusteringInformationResponse> {
    let mut dummy_session = SessionManager::instance()
        .create_session(SessionType::Dummy)
        .await?;
    dummy_session.set_current_tenant(tenant.clone())?;
    let session = SessionManager::instance().register_session(dummy_session)?;

    let ctx: Arc<dyn TableContext> = session
        .create_query_context(&databend_common_version::BUILD_INFO)
        .await?;
    let tbl = ctx
        .get_table_with_branch("default", database, table, params.branch.as_deref())
        .await?;
    let tbl = FuseTable::try_from_table(tbl.as_ref())?;
    get_clustering_information(ctx, tbl, &params.cluster_key).await
}

fn clustering_information_error(error: ErrorCode) -> poem::Error {
    let status = match error.name().as_str() {
        "UnknownDatabase" | "UnknownTable" => StatusCode::NOT_FOUND,
        "BadArguments" | "StorageUnsupported" | "UnclusteredTable" => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    poem::Error::from_string(error.to_string(), status)
}
