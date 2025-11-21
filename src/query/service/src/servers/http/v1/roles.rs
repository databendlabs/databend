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
use databend_common_users::BUILTIN_ROLE_PUBLIC;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListRolesResponse {
    pub roles: Vec<RoleInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RoleInfo {
    pub name: String,
    pub is_current: bool,
    pub is_default: bool,
}

/// same as `show_roles` in
/// src/query/service/src/table_functions/show_roles/show_roles_table.rs
#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext) -> Result<ListRolesResponse> {
    let mut all_roles = ctx.session.get_all_available_roles().await?;
    all_roles.sort_by(|a, b| a.name.cmp(&b.name));
    let current_user = ctx.session.get_current_user()?;
    let current_role = ctx
        .session
        .get_current_role()
        .map_or(BUILTIN_ROLE_PUBLIC.to_string(), |role| role.name);
    let default_role = current_user
        .option
        .default_role()
        .map_or(BUILTIN_ROLE_PUBLIC.to_string(), |role| role.to_string());
    let mut roles = vec![];
    for role in all_roles {
        let is_current = role.name == current_role;
        let is_default = role.name == default_role;
        roles.push(RoleInfo {
            name: role.name,
            is_current,
            is_default,
        });
    }
    if roles.is_empty() {
        roles.push(RoleInfo {
            name: BUILTIN_ROLE_PUBLIC.to_string(),
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
