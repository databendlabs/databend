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

use jwt_simple::prelude::Deserialize;
use jwt_simple::prelude::Serialize;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;

use crate::servers::http::v1::session::token_manager::TokenManager;
use crate::servers::http::v1::session::token_manager::TokenPair;
use crate::servers::http::v1::session::token_manager::REFRESH_TOKEN_VALIDITY_IN_SECS;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::QueryError;

#[derive(Deserialize, Clone)]
struct RenewRequest {
    pub session_token: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RenewResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token_validity_in_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<QueryError>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn renew_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<RenewRequest>,
) -> PoemResult<impl IntoResponse> {
    let refresh_token = ctx
        .databend_token
        .as_ref()
        .expect("/session/renew should be authed by databend token")
        .clone();
    match TokenManager::instance()
        .new_token_pair(
            &ctx.session,
            Some(TokenPair {
                session: req.session_token,
                refresh: refresh_token,
            }),
        )
        .await
    {
        Ok((_, token_pair)) => Ok(Json(RenewResponse {
            session_token: Some(token_pair.session.clone()),
            refresh_token: Some(token_pair.refresh.clone()),
            refresh_token_validity_in_secs: Some(REFRESH_TOKEN_VALIDITY_IN_SECS),
            error: None,
        })),
        Err(e) => Ok(Json(RenewResponse {
            session_token: None,
            refresh_token: None,
            refresh_token_validity_in_secs: None,
            error: Some(QueryError::from_error_code(e)),
        })),
    }
}