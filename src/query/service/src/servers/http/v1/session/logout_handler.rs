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

use jwt_simple::prelude::Serialize;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;

use crate::servers::http::v1::session::client_session_manager::ClientSessionManager;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::QueryError;

#[derive(Serialize, Debug, Clone)]
pub struct LogoutResponse {
    error: Option<QueryError>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn logout_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    let error = if let Some(token) = &ctx.databend_token {
        ClientSessionManager::instance()
            .drop_client_session(token)
            .await
            .map_err(QueryError::from_error_code)
            .err()
    } else {
        // should not get here since request is already authed
        Some(QueryError {
            code: 500,
            message: "missing session token".to_string(),
            detail: "".to_string(),
        })
    };
    Ok(Json(LogoutResponse { error }))
}
