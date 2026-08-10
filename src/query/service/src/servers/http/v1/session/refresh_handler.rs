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

use databend_common_exception::ErrorCode;
use jwt_simple::prelude::Deserialize;
use jwt_simple::prelude::Serialize;
use poem::IntoResponse;
use poem::error::Result as PoemResult;
use poem::web::Json;

use crate::auth::Credential;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::session::client_session_manager::ClientSessionManager;
use crate::servers::http::v1::session::login_handler::TokensInfo;

#[derive(Deserialize, Clone)]
struct RefreshRequest {
    // to drop the old token earlier instead of waiting for expiration
    pub session_token: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct RefreshResponse {
    tokens: TokensInfo,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn refresh_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<RefreshRequest>,
) -> PoemResult<impl IntoResponse> {
    let mgr = ClientSessionManager::instance();
    match &ctx.credential {
        Credential::DatabendToken { token, .. } => {
            let client_session_id = ctx
                .client_session_id
                .as_ref()
                .ok_or_else(|| {
                    HttpErrorCode::bad_request(ErrorCode::AuthenticateFailure(
                        "refresh endpoint requires a client session ID",
                    ))
                })?
                .clone();
            let (_, token_pair) = mgr
                .new_token_pair(
                    &ctx.session,
                    client_session_id,
                    Some(token.clone()),
                    req.session_token,
                )
                .await
                .map_err(HttpErrorCode::server_error)?;
            Ok(Json(RefreshResponse {
                tokens: TokensInfo {
                    session_token_ttl_in_secs: ClientSessionManager::instance()
                        .max_idle_time
                        .as_secs(),
                    session_token: token_pair.session.clone(),
                    refresh_token: token_pair.refresh.clone(),
                },
            }))
        }
        _ => Err(HttpErrorCode::bad_request(ErrorCode::AuthenticateFailure(
            "refresh endpoint requires a Databend refresh token, not a JWT or key-pair token",
        ))
        .into()),
    }
}
