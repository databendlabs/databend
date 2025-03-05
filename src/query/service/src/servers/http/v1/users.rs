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
use databend_common_users::UserApiProvider;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListUsersResponse {
    users: Vec<UserInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserInfo {
    name: String,
    hostname: String,
    auth_type: String,
    default_role: String,
    roles: Vec<String>,
    disabled: bool,
    network_policy: Option<String>,
    password_policy: Option<String>,
    must_change_password: Option<bool>,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext) -> Result<ListUsersResponse> {
    let tenant = ctx.session.get_current_tenant();
    let user_api = UserApiProvider::instance();
    let mut users = vec![];
    for user in user_api.get_users(&tenant).await? {
        users.push(UserInfo {
            name: user.name.clone(),
            hostname: user.hostname.clone(),
            auth_type: user.auth_info.get_type().to_str().to_string(),
            default_role: user.option.default_role().cloned().unwrap_or_default(),
            roles: user.grants.roles(),
            disabled: user.option.disabled().cloned().unwrap_or_default(),
            network_policy: user.option.network_policy().cloned(),
            password_policy: user.option.password_policy().cloned(),
            must_change_password: user.option.must_change_password().cloned(),
        });
    }
    Ok(ListUsersResponse { users })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_users_handler(ctx: &HttpQueryContext) -> PoemResult<impl IntoResponse> {
    let resp = handle(ctx).await.map_err(InternalServerError)?;
    Ok(Json(resp))
}
