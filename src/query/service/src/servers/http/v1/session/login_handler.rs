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

use crate::auth::Credential;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::session::client_session_manager::ClientSessionManager;
use crate::servers::http::v1::session::consts::SESSION_TOKEN_TTL;
use crate::servers::http::v1::HttpQueryContext;

#[derive(Deserialize, Clone)]
struct LoginRequest {
    pub database: Option<String>,
    pub role: Option<String>,
    pub settings: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Debug, Clone)]
pub struct LoginResponse {
    version: String,
    session_id: String,
    session_token_ttl_in_secs: u64,

    /// for now, only use session token when authed by user-password
    session_token: Option<String>,
    refresh_token: Option<String>,
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

    if let Some(conf_settings) = &req.settings {
        let settings = session.get_settings();
        for (k, v) in conf_settings {
            settings.set_setting(k.to_string(), v.to_string())?;
        }
    }
    Ok(())
}

///  # For client/driver developer:
/// - It is encouraged to call `/v1/session/login` when establishing connection, not mandatory for now.
/// - May get 404 when talk to old server, may check `/health` (no `/v1` prefix) to ensure the host:port is not wrong.
#[poem::handler]
#[async_backtrace::framed]
pub async fn login_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<LoginRequest>,
) -> PoemResult<impl IntoResponse> {
    let version = QUERY_SEMVER.to_string();
    check_login(ctx, &req)
        .await
        .map_err(HttpErrorCode::bad_request)?;

    match ctx.credential {
        Credential::Jwt { .. } => {
            let session_id = uuid::Uuid::new_v4().to_string();
            Ok(Json(LoginResponse {
                version,
                session_id,
                session_token_ttl_in_secs: SESSION_TOKEN_TTL.as_secs(),
                session_token: None,
                refresh_token: None,
            }))
        }
        Credential::Password { .. } => {
            let (session_id, token_pair) = ClientSessionManager::instance()
                .new_token_pair(&ctx.session, None, None)
                .await
                .map_err(HttpErrorCode::server_error)?;
            Ok(Json(LoginResponse {
                version,
                session_id,

                session_token_ttl_in_secs: SESSION_TOKEN_TTL.as_secs(),
                session_token: Some(token_pair.session.clone()),
                refresh_token: Some(token_pair.refresh.clone()),
            }))
        }
        _ => unreachable!("/session/login expect password or JWT"),
    }
}
