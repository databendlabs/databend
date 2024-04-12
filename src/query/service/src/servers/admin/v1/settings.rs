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
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use minitrace::func_name;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SettingsItem {
    pub name: String,
    pub desc: &'static str,
    pub level: String,
    pub user_value: String,
    pub default_value: String,
    pub range: Option<String>,
}

async fn list_settings_impl(tenant: &str) -> Result<Vec<SettingsItem>> {
    let settings = Settings::create(Tenant::new_or_err(tenant, func_name!())?);
    settings.load_changes().await?;

    Ok(settings
        .into_iter()
        .map(|item| SettingsItem {
            name: item.name,
            desc: item.desc,
            level: format!("{:?}", item.level),
            user_value: item.user_value.to_string(),
            default_value: item.default_value.to_string(),
            range: item.range.map(|x| x.to_string()),
        })
        .collect::<Vec<_>>())
}

async fn set_setting_impl(tenant: &str, key: &str, value: String) -> Result<Vec<SettingsItem>> {
    if tenant.is_empty() {
        return Err(ErrorCode::TenantIsEmpty(
            "Tenant can not empty(while set setting)",
        ));
    }

    if key.is_empty() || key.len() > 1024 {
        return Err(ErrorCode::BadArguments(
            "Setting key is empty or large length(while set setting)",
        ));
    }

    let settings = Settings::create(Tenant::new_or_err(tenant, func_name!())?);
    settings.set_global_setting(key.to_string(), value).await?;

    Ok(settings
        .into_iter()
        .map(|item| SettingsItem {
            name: item.name,
            desc: item.desc,
            level: format!("{:?}", item.level),
            user_value: item.user_value.to_string(),
            default_value: item.default_value.to_string(),
            range: item.range.map(|x| x.to_string()),
        })
        .collect::<Vec<_>>())
}

async fn unset_setting_impl(tenant: &str, key: &str) -> Result<Vec<SettingsItem>> {
    if tenant.is_empty() {
        return Err(ErrorCode::TenantIsEmpty(
            "Tenant can not empty(while unset setting)",
        ));
    }

    if key.is_empty() || key.len() > 1024 {
        return Err(ErrorCode::BadArguments(
            "Setting key is empty or large length(while unset setting)",
        ));
    }

    let settings = Settings::create(Tenant::new_or_err(tenant, func_name!())?);
    settings.try_drop_global_setting(key).await?;

    Ok(settings
        .into_iter()
        .map(|item| SettingsItem {
            name: item.name,
            desc: item.desc,
            level: format!("{:?}", item.level),
            user_value: item.user_value.to_string(),
            default_value: item.default_value.to_string(),
            range: item.range.map(|x| x.to_string()),
        })
        .collect::<Vec<_>>())
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_settings(Path(tenant): Path<String>) -> poem::Result<impl IntoResponse> {
    Ok(Json(
        list_settings_impl(&tenant)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn set_settings(
    Path((tenant, key)): Path<(String, String)>,
    value: Json<String>,
) -> poem::Result<impl IntoResponse> {
    Ok(Json(
        set_setting_impl(&tenant, &key, value.0)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn unset_settings(
    Path((tenant, key)): Path<(String, String)>,
) -> poem::Result<impl IntoResponse> {
    Ok(Json(
        unset_setting_impl(&tenant, &key)
            .await
            .map_err(poem::error::InternalServerError)?,
    ))
}
