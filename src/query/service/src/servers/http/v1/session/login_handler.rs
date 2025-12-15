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

use databend_common_storages_fuse::TableContext;
use jwt_simple::prelude::Deserialize;
use jwt_simple::prelude::Serialize;
use poem::IntoResponse;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Query;

use crate::auth::Credential;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::session::client_session_manager::ClientSessionManager;

#[derive(Deserialize, Clone)]
struct LoginRequest {
    pub database: Option<String>,
    pub role: Option<String>,
    pub settings: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Clone, Debug)]
pub(crate) struct TokensInfo {
    pub(crate) session_token_ttl_in_secs: u64,
    pub(crate) session_token: String,
    pub(crate) refresh_token: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct LoginResponse {
    version: String,
    session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokens: Option<TokensInfo>,
}

/// Although theses can be checked for each /v1/query for now,
/// getting these error in `conn()` instead of `execute()` is more in line with what users expect.
async fn check_login(
    ctx: &HttpQueryContext,
    req: &LoginRequest,
) -> databend_common_exception::Result<()> {
    let session = &ctx.session;
    let table_ctx = session.create_query_context(ctx.version).await?;
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

#[derive(Deserialize)]
struct LoginQuery {
    disable_session_token: Option<bool>,
}

///  # For client/driver developer:
/// - It is encouraged to call `/v1/session/login` when establishing connection, not mandatory for now.
/// - May get 404 when talk to old server, may check `/health` (no `/v1` prefix) to ensure the host:port is not wrong.
#[poem::handler]
#[async_backtrace::framed]
pub async fn login_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<LoginRequest>,
    Query(query): Query<LoginQuery>,
) -> PoemResult<impl IntoResponse> {
    let session_id = ctx
        .client_session_id
        .as_ref()
        .expect("login_handler expect session id in ctx")
        .clone();
    check_login(ctx, &req)
        .await
        .map_err(HttpErrorCode::bad_request)?;
    let version = &ctx.version.semantic;
    let id_only = || {
        Ok(Json(LoginResponse {
            version: version.to_string(),
            session_id: session_id.clone(),
            tokens: None,
        }))
    };

    match ctx.credential {
        Credential::Jwt { .. } => id_only(),
        Credential::Password { .. } if query.disable_session_token.unwrap_or(false) => id_only(),
        Credential::Password { .. } => {
            let (session_id, token_pair) = ClientSessionManager::instance()
                .new_token_pair(&ctx.session, session_id, None, None)
                .await
                .map_err(HttpErrorCode::server_error)?;
            Ok(Json(LoginResponse {
                version: version.to_string(),
                session_id,
                tokens: Some(TokensInfo {
                    session_token_ttl_in_secs: ClientSessionManager::instance()
                        .max_idle_time
                        .as_secs(),
                    session_token: token_pair.session.clone(),
                    refresh_token: token_pair.refresh.clone(),
                }),
            }))
        }
        _ => unreachable!("/session/login endpoint requires password or JWT authentication"),
    }
}
