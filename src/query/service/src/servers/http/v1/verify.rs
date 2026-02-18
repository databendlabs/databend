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

use databend_common_users::UserApiProvider;
use jwt_simple::prelude::Serialize;
use poem::IntoResponse;
use poem::error::Result as PoemResult;
use poem::web::Json;

use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Debug, Clone)]
pub struct VerifyResponse {
    tenant: String,
    user: String,
    auth_type: String,
    is_configured: bool,
    default_role: String,
    default_warehouse: String,
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
    let is_configured = UserApiProvider::instance()
        .get_configured_user(&user.name)
        .is_some();
    let auth_type = user.auth_info.get_type().to_str().to_string();
    let default_role = user.option.default_role().cloned().unwrap_or_default();
    let default_warehouse = user.option.default_warehouse().cloned().unwrap_or_default();
    let roles = ctx
        .session
        .get_all_effective_roles()
        .await
        .map_err(HttpErrorCode::server_error)?
        .into_iter()
        .map(|r| r.name)
        .collect();
    Ok(Json(VerifyResponse {
        tenant: tenant.tenant_name().to_string(),
        user: user.name,
        is_configured,
        auth_type,
        default_role,
        default_warehouse,
        roles,
    }))
}
