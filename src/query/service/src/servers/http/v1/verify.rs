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

use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Debug, Clone)]
pub struct VerifyResponse {
    tenant: String,
    user: String,
    roles: Vec<String>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn verify_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    let tenant = ctx.session.get_current_tenant();
    let user = ctx
        .session
        .get_current_user()
        .map_err(HttpErrorCode::server_error)?;
    let roles = user.grants.roles();
    Ok(Json(VerifyResponse {
        tenant: tenant.tenant_name().to_string(),
        user: user.identity().display().to_string(),
        roles,
    }))
}
