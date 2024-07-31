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

use std::collections::BTreeMap;

use databend_common_config::QUERY_SEMVER;
use databend_common_storages_fuse::TableContext;
use jwt_simple::prelude::Deserialize;
use jwt_simple::prelude::Serialize;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;

use crate::servers::http::v1::session::token_manager::TokenManager;
use crate::servers::http::v1::session::token_manager::REFRESH_TOKEN_VALIDITY_IN_SECS;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::QueryError;

#[derive(Deserialize, Clone)]
struct LoginRequest {
    pub database: Option<String>,
    pub role: Option<String>,
    pub secondary_roles: Option<Vec<String>>,
    pub settings: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LoginResponse {
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token_validity_in_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<QueryError>,
}

/// Although theses can be checked for each /v1/query for now,
/// getting these error in `conn()` instead of `execute()` is more in line with what users expect.
async fn check_login(
    ctx: &HttpQueryContext,
    req: &LoginRequest,
) -> databend_common_exception::Result<()> {
    let session = &ctx.session;
    let table_ctx = session.create_query_context().await?;
    if let Some(database) = &req.database {
        let cat = session.get_current_catalog();
        let cat = table_ctx.get_catalog(&cat).await?;
        cat.get_database(&ctx.session.get_current_tenant(), database)
            .await?;
    }
    if let Some(role_name) = &req.role {
        session.set_current_role_checked(role_name).await?;
    }
    session
        .set_secondary_roles_checked(req.secondary_roles.clone())
        .await?;

    if let Some(conf_settings) = &req.settings {
        let settings = session.get_settings();
        for (k, v) in conf_settings {
            settings.set_setting(k.to_string(), v.to_string())?;
        }
    }
    Ok(())
}

///  # For SQL driver implementer:
/// - It is encouraged to call `/v1/session/login` when establishing connection, not mandatory for now.
/// - May get 404 when talk to old server, may check `/health` (no `/v1` prefix) to ensure the host:port is not wrong.
///
///  # TODO (need design):
/// - (optional) check client version.
/// - Return token for auth in the following queries from this session, to make it a real login.
#[poem::handler]
#[async_backtrace::framed]
pub async fn login_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<LoginRequest>,
) -> PoemResult<impl IntoResponse> {
    let version = QUERY_SEMVER.to_string();
    let error = check_login(ctx, &req)
        .await
        .map_err(QueryError::from_error_code)
        .err();

    match TokenManager::instance()
        .new_token_pair(&ctx.session, None)
        .await
    {
        Ok((session_id, token_pair)) => Ok(Json(LoginResponse {
            version,
            session_id: Some(session_id),
            session_token: Some(token_pair.session),
            refresh_token: Some(token_pair.refresh),
            refresh_token_validity_in_secs: Some(REFRESH_TOKEN_VALIDITY_IN_SECS),
            error,
        })),
        Err(e) => Ok(Json(LoginResponse {
            version,
            session_id: None,
            session_token: None,
            refresh_token: None,
            refresh_token_validity_in_secs: None,
            error: Some(QueryError::from_error_code(e)),
        })),
    }
}
