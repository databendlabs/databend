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

use databend_common_exception::Result;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListRolesResponse {
    roles: Vec<RoleInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RoleInfo {
    name: String,
    is_current: bool,
    is_default: bool,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext) -> Result<ListRolesResponse> {
    let user = ctx.session.get_current_user()?;
    let current_role = ctx
        .session
        .get_current_role()
        .map_or("public".to_string(), |role| role.name);
    let default_role = user
        .option
        .default_role()
        .map_or("public".to_string(), |role| role.to_string());
    let mut roles = vec![];
    for role in user.grants.roles() {
        let is_current = role == current_role;
        let is_default = role == default_role;
        roles.push(RoleInfo {
            name: role.clone(),
            is_current,
            is_default,
        });
    }
    if roles.is_empty() {
        roles.push(RoleInfo {
            name: "public".to_string(),
            is_current: true,
            is_default: true,
        });
    }
    Ok(ListRolesResponse { roles })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_roles_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx).await.map_err(InternalServerError)?;
    Ok(Json(resp))
}
