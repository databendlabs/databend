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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::UriLocation;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_common_storage::StagePathTraversalPolicy;
use databend_common_storage::ensure_no_stage_path_traversal;
use databend_common_storage::is_stage_path_traversal;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use log::LevelFilter;
use log::debug;

use crate::binder::insert::STAGE_PLACEHOLDER;
use crate::binder::location::apply_uri_connection;
use crate::binder::location::get_storage_params_from_options;
use crate::binder::location::parse_storage_params_from_uri;
use crate::binder::location::parse_uri_location;

/// location can be:
/// - mystage
/// - mystage/
/// - mystage/abc
/// - ~/abc
///
/// Returns the stage name and relative path towards the stage's root.
///
/// If input location is empty we will convert it to `/` means the root of stage
///
/// - mystage => (mystage, "/")
///
/// If input location is endswith `/`, it's a folder.
///
/// - mystage/ => (mystage, "/")
///
/// Otherwise, it's a file
///
/// - mystage/abc => (mystage, "abc")
///
/// - ~/abc => ("~", "abc")
pub fn parse_stage_location(location: &str) -> Result<(String, String)> {
    let location = location.trim_start_matches('@');
    let names: Vec<&str> = location.splitn(2, '/').filter(|v| !v.is_empty()).collect();
    if names.is_empty() {
        return Err(ErrorCode::BadArguments(
            "stage path must include a stage name".to_string(),
        ));
    }
    if names[0] == STAGE_PLACEHOLDER {
        return Err(ErrorCode::BadArguments(
            "placeholder @_databend_upload as location: should be used in streaming_load handler or replaced in client.",
        ));
    }

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');
    let path = if path.is_empty() { "/" } else { path };
    Ok((names[0].to_string(), path.to_string()))
}

pub fn parse_stage_name(location: &str) -> Result<String> {
    if !location.starts_with('@') {
        return Err(ErrorCode::BadArguments(format!(
            "stage path must start with @, but got {}",
            location
        )));
    }
    let stage_name = location.trim_start_matches('@');
    if stage_name.is_empty() {
        return Err(ErrorCode::BadArguments(
            "stage path must include a stage name".to_string(),
        ));
    }
    if stage_name == STAGE_PLACEHOLDER {
        return Err(ErrorCode::BadArguments(
            "placeholder @_databend_upload as location: should be used in streaming_load handler or replaced in client.".to_string(),
        ));
    }
    if stage_name.contains('/') {
        return Err(ErrorCode::BadArguments(format!(
            "stage argument must be a stage name, but got {}",
            location
        )));
    }
    Ok(stage_name.to_string())
}

pub fn validate_stage_path_traversal(
    _settings: &Settings,
    path: &str,
    is_write: bool,
) -> Result<()> {
    let policy = databend_common_config::GlobalConfig::instance()
        .storage
        .stage_path_traversal_policy;
    validate_stage_path_traversal_policy(policy, path, is_write)
}

pub fn validate_stage_path_traversal_policy(
    policy: StagePathTraversalPolicy,
    path: &str,
    is_write: bool,
) -> Result<()> {
    if !is_stage_path_traversal(path) {
        return Ok(());
    }

    let allowed = if is_write {
        policy.allows_write()
    } else {
        policy.allows_read()
    };

    if allowed {
        Ok(())
    } else {
        ensure_no_stage_path_traversal(path)
    }
}

pub fn validate_stage_files_path_traversal(
    settings: &Settings,
    path: &str,
    files: Option<&[String]>,
    is_write: bool,
) -> Result<()> {
    validate_stage_path_traversal(settings, path, is_write)?;
    if let Some(files) = files {
        for file in files {
            validate_stage_path_traversal(settings, file, is_write)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub enum StagePathAccess {
    Read,
    Write,
}

impl StagePathAccess {
    fn is_write(self) -> bool {
        matches!(self, StagePathAccess::Write)
    }
}

#[derive(Clone)]
pub struct StageResolver<A = ()> {
    tenant: Tenant,
    current_user: UserIdentity,
    user_api_provider: Arc<UserApiProvider>,
    storage_allow_insecure: bool,
    settings: Arc<Settings>,
    authorization: A,
}

impl StageResolver<Arc<dyn TableContext>> {
    pub fn from_table_context(
        ctx: Arc<dyn TableContext>,
        user_api_provider: Arc<UserApiProvider>,
        storage_allow_insecure: bool,
    ) -> Result<Self> {
        Self::from_authorization(
            ctx.get_tenant(),
            ctx.get_current_user()?.identity(),
            ctx.get_settings(),
            ctx,
            user_api_provider,
            storage_allow_insecure,
        )
    }
}

impl<A> StageResolver<Arc<A>>
where A: TableContextAuthorization + ?Sized + 'static
{
    pub fn from_authorization(
        tenant: Tenant,
        current_user: UserIdentity,
        settings: Arc<Settings>,
        authorization: Arc<A>,
        user_api_provider: Arc<UserApiProvider>,
        storage_allow_insecure: bool,
    ) -> Result<Self> {
        Ok(Self {
            tenant,
            current_user,
            user_api_provider,
            storage_allow_insecure,
            settings,
            authorization,
        })
    }
}

impl<'a, A> StageResolver<&'a A>
where A: TableContextAuthorization + ?Sized
{
    pub fn from_authorization_ref(
        tenant: Tenant,
        current_user: UserIdentity,
        settings: Arc<Settings>,
        authorization: &'a A,
        user_api_provider: Arc<UserApiProvider>,
        storage_allow_insecure: bool,
    ) -> Result<Self> {
        Ok(Self {
            tenant,
            current_user,
            user_api_provider,
            storage_allow_insecure,
            settings,
            authorization,
        })
    }
}

impl<A> StageResolver<A>
where
    A: std::ops::Deref,
    A::Target: TableContextAuthorization,
{
    pub async fn resolve_connection(&self, name: &str) -> Result<UserDefinedConnection> {
        if self
            .settings
            .get_enable_experimental_connection_privilege_check()?
        {
            let visibility_checker = self
                .authorization
                .get_visibility_checker(false, Object::Connection)
                .await?;
            if !visibility_checker.check_connection_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege AccessConnection is required on connection {name} for user {}",
                    self.current_user.display(),
                )));
            }
        }

        self.user_api_provider
            .get_connection(&self.tenant, name)
            .await
    }

    #[async_backtrace::framed]
    pub async fn resolve_storage_params_from_uri(
        &self,
        uri: &mut UriLocation,
        usage: &str,
    ) -> Result<StorageParams> {
        self.resolve_uri_connection(uri).await?;
        Ok(parse_storage_params_from_uri(uri, usage).await?)
    }

    #[async_backtrace::framed]
    pub async fn resolve_storage_params_from_options(
        &self,
        options: &BTreeMap<String, String>,
    ) -> Result<StorageParams> {
        let connection = if let Some(name) = options.get("connection_name") {
            Some(self.resolve_connection(name).await?)
        } else {
            None
        };
        get_storage_params_from_options(options, connection).await
    }

    #[async_backtrace::framed]
    pub async fn resolve_uri_location(
        &self,
        uri: &mut UriLocation,
    ) -> Result<(StorageParams, String)> {
        self.resolve_uri_connection(uri).await?;
        Ok(parse_uri_location(uri).await?)
    }

    async fn resolve_uri_connection(&self, uri: &mut UriLocation) -> Result<()> {
        if let Some(name) = uri.connection.get("connection_name").cloned() {
            let conn = self.resolve_connection(&name).await.map_err(|err| {
                ErrorCode::BadArguments(format!("fail to get connection_name {name}: {err:?}"))
            })?;
            apply_uri_connection(uri, &name, conn)?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn resolve_file_location(
        &self,
        location: &FileLocation,
        access: StagePathAccess,
    ) -> Result<(StageInfo, String)> {
        match location.clone() {
            FileLocation::Stage(location) => self.resolve_stage_location(&location, access).await,
            FileLocation::Uri(mut uri) => {
                let (storage_params, path) = self.resolve_uri_location(&mut uri).await?;
                if !storage_params.is_secure() && !self.storage_allow_insecure {
                    Err(ErrorCode::StorageInsecure(
                        "copy from insecure storage is not allowed",
                    ))
                } else {
                    let stage_info = StageInfo::new_external_stage(storage_params, true);
                    Ok((stage_info, path))
                }
            }
        }
    }
}

impl StageResolver {
    pub fn from_table_context_for_stage(
        ctx: &dyn TableContext,
        user_api_provider: Arc<UserApiProvider>,
        storage_allow_insecure: bool,
    ) -> Result<Self> {
        Ok(Self {
            tenant: ctx.get_tenant(),
            current_user: ctx.get_current_user()?.identity(),
            user_api_provider,
            storage_allow_insecure,
            settings: ctx.get_settings(),
            authorization: (),
        })
    }
}

impl<A> StageResolver<A> {
    #[async_backtrace::framed]
    pub async fn resolve_stage_location(
        &self,
        location: &str,
        access: StagePathAccess,
    ) -> Result<(StageInfo, String)> {
        let (stage_name, path) = parse_stage_location(location)?;

        let mut stage = if stage_name == "~" {
            StageInfo::new_user_stage(&self.current_user.username)
        } else {
            self.user_api_provider
                .get_stage(&self.tenant, &stage_name)
                .await?
        };
        if ThreadTracker::capture_log_settings()
            .is_some_and(|settings| settings.level == LevelFilter::Off)
        {
            // History log transform queries use the internal history stage.
            // Enable credential chain at runtime since the flag is not persisted in meta.
            stage.allow_credential_chain = true;
        }

        validate_stage_path_traversal(self.settings.as_ref(), &path, access.is_write())?;

        debug!("parsed stage: {stage:?}, path: {path}");
        Ok((stage, path))
    }

    #[async_backtrace::framed]
    pub async fn resolve_stage_locations(
        &self,
        locations: &[String],
        access: StagePathAccess,
    ) -> Result<Vec<(StageInfo, String)>> {
        let mut results = Vec::with_capacity(locations.len());
        for location in locations {
            results.push(self.resolve_stage_location(location, access).await?);
        }
        Ok(results)
    }
}
