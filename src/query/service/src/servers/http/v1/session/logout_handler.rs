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
use log::info;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;

use crate::auth::Credential;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::error::QueryError;
use crate::servers::http::v1::session::client_session_manager::ClientSessionManager;
use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Debug, Clone)]
pub struct LogoutResponse {
    error: Option<QueryError>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn logout_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    if let Some(id) = &ctx.client_session_id {
        info!(
            "Logout request: user={}, client_session_id={}, credential_type={:?}",
            ctx.user_name,
            id,
            ctx.credential.type_name()
        );
        ClientSessionManager::instance()
            .drop_client_session_state(&ctx.session.get_current_tenant(), &ctx.user_name, id)
            .await
            .map_err(HttpErrorCode::server_error)?;
        if let Credential::DatabendToken { token, .. } = &ctx.credential {
            ClientSessionManager::instance()
                .drop_client_session_token(token)
                .await
                .map_err(HttpErrorCode::server_error)?;
        };
    }
    return Ok(Json(LogoutResponse { error: None }));
}
