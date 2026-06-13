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
use databend_common_exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::schema::CreateOption;
use databend_common_users::UserApiProvider;
use poem::IntoResponse;
use poem::error::BadRequest;
use poem::error::Forbidden;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::http::v1::HttpQueryContext;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateUserRequest {
    pub name: String,
    pub hostname: Option<String>,
    pub auth_type: Option<String>,
    pub auth_string: Option<String>,
    pub default_role: Option<String>,
    pub default_warehouse: Option<String>,
    pub roles: Option<Vec<String>>,
    pub grant_all: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ListUsersResponse {
    pub users: Vec<UserItem>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserItem {
    pub name: String,
    pub hostname: String,
    pub auth_type: String,
    pub default_role: String,
    pub default_warehouse: String,
    pub grant_roles: Vec<String>,
    pub disabled: bool,
    pub network_policy: Option<String>,
    pub password_policy: Option<String>,
    pub must_change_password: Option<bool>,
}

#[async_backtrace::framed]
async fn handle(ctx: &HttpQueryContext) -> Result<ListUsersResponse> {
    let tenant = ctx.session.get_current_tenant();
    let user_api = UserApiProvider::instance();
    let mut users = vec![];
    for user in user_api.get_users(&tenant).await? {
        users.push(UserItem {
            name: user.name.clone(),
            hostname: user.hostname.clone(),
            auth_type: user.auth_info.get_type().to_str().to_string(),
            default_role: user.option.default_role().cloned().unwrap_or_default(),
            default_warehouse: user.option.default_warehouse().cloned().unwrap_or_default(),
            grant_roles: user.grants.roles_vec(),
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

#[async_backtrace::framed]
async fn create_user(ctx: &HttpQueryContext, req: CreateUserRequest) -> Result<()> {
    let user = ctx.session.get_current_user()?;
    if !user.is_account_admin() {
        return Err(ErrorCode::PermissionDenied(
            "[HTTP-USERS] Permission denied: only account admin can create or update users",
        ));
    }
    let user_api = UserApiProvider::instance();
    let auth_info = AuthInfo::create(&req.auth_type, &req.auth_string).map_err(|e| {
        ErrorCode::InvalidArgument(format!(
            "[HTTP-USERS] Invalid authentication information: {}",
            e
        ))
    })?;
    let mut user_info = UserInfo::new(
        &req.name,
        &req.hostname.unwrap_or("%".to_string()),
        auth_info,
    );
    user_info.option = user_info
        .option
        .with_default_role(req.default_role)
        .with_default_warehouse(req.default_warehouse);
    if let Some(roles) = req.roles {
        for role in roles {
            user_info.grants.grant_role(role);
        }
    }
    if req.grant_all.unwrap_or(false) {
        user_info.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );
    }
    let tenant = ctx.session.get_current_tenant();
    user_api
        .create_user(&tenant, user_info, &CreateOption::CreateOrReplace)
        .await?;
    Ok(())
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn create_user_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<CreateUserRequest>,
) -> PoemResult<impl IntoResponse> {
    create_user(ctx, req).await.map_err(|e| match e.code() {
        ErrorCode::PERMISSION_DENIED => Forbidden(e),
        ErrorCode::INVALID_ARGUMENT => BadRequest(e),
        _ => InternalServerError(e),
    })?;
    Ok(Json(()))
}
