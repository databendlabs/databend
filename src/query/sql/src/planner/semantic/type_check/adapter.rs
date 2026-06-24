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

use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::ast::Query;
use databend_common_base::runtime::block_on_with_handle;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_settings::Settings;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_common_users::security_policy_cache::CachedSecurityPolicy;
use databend_common_users::security_policy_cache::PolicyType;
use databend_common_users::security_policy_cache::RawPolicyDef;
use databend_common_users::security_policy_cache::SecurityPolicyCacheManager;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use itertools::Itertools;
use serde_json::json;
use serde_json::to_string;

use super::AuthFunction;
use super::FullTypeCheckAdapter;
use super::FullTypeCheckAdapterDependencies;
use super::NamespaceFunction;
use super::SessionFunction;
use super::TypeCheckAdapter;
use super::TypeCheckDictionary;
use super::TypeCheckSubqueryPlan;
use super::TypeChecker;
use crate::BindContext;
use crate::DefaultExprBinder;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::binder::StagePathAccess;
use crate::binder::StageResolver;
use crate::plans::DictGetFunctionArgument;
use crate::plans::DictionarySource;
use crate::plans::RedisSource;
use crate::plans::ScalarExpr;
use crate::plans::SqlSource;

impl FullTypeCheckAdapter {
    pub fn new(ctx: Arc<dyn TableContext>) -> Result<Self> {
        let dependencies = FullTypeCheckAdapterDependencies::from_global();
        Ok(Self {
            ctx,
            dependencies,
            forbid_udf: false,
            skip_sequence_check: false,
        })
    }

    pub fn with_forbid_udf(mut self, forbid_udf: bool) -> Self {
        self.forbid_udf = forbid_udf;
        self
    }

    pub fn with_skip_sequence_check(mut self, skip_sequence_check: bool) -> Self {
        self.skip_sequence_check = skip_sequence_check;
        self
    }

    pub(super) fn block_on<F, T>(&self, future: F) -> Result<T>
    where F: std::future::Future<Output = Result<T>> {
        let handle = (self.dependencies.async_runtime_handle)()?;
        block_on_with_handle(&handle, future)
    }
}

impl FullTypeCheckAdapterDependencies {
    fn from_global() -> Self {
        let global_config = GlobalConfig::instance();
        let cloud_control_api_provider = if global_config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_some()
        {
            Some(CloudControlApiProvider::instance())
        } else {
            None
        };

        fn current_async_runtime_handle() -> Result<tokio::runtime::Handle> {
            tokio::runtime::Handle::try_current()
                .map_err(|_| ErrorCode::Internal("type check operation requires a tokio runtime"))
        }

        Self {
            async_runtime_handle: current_async_runtime_handle,
            aggregate_function_factory: AggregateFunctionFactory::instance(),
            license_manager: LicenseManagerSwitch::instance(),
            catalog_manager: CatalogManager::instance(),
            user_api_provider: UserApiProvider::instance(),
            storage_allow_insecure: global_config.storage.allow_insecure,
            security_policy_cache_manager: SecurityPolicyCacheManager::instance(),
            cloud_control_api_provider,
        }
    }
}

impl TypeCheckAdapter for FullTypeCheckAdapter {
    type UdfAdapter = Self;

    fn function_context(&self) -> Result<FunctionContext> {
        self.ctx.get_function_context()
    }

    fn settings(&self) -> Arc<Settings> {
        self.ctx.get_settings()
    }

    fn aggregate_function_factory(&self) -> &'static AggregateFunctionFactory {
        self.dependencies.aggregate_function_factory
    }

    fn udf_adapter(&self) -> Result<Self::UdfAdapter> {
        Ok(self.clone())
    }

    fn forbid_udf(&self) -> bool {
        self.forbid_udf
    }

    fn validate_sequence(&self, sequence_name: &str) -> Result<()> {
        if self.skip_sequence_check {
            return Ok(());
        }

        let catalog = self.ctx.get_default_catalog()?;
        let req = GetSequenceReq {
            ident: SequenceIdent::new(self.ctx.get_tenant(), sequence_name.to_string()),
        };

        let visibility_checker = if self
            .ctx
            .get_settings()
            .get_enable_experimental_sequence_privilege_check()?
        {
            let ctx = self.ctx.clone();
            Some(self.block_on(async move {
                ctx.get_visibility_checker(false, Object::Sequence).await
            })?)
        } else {
            None
        };
        self.block_on(catalog.get_sequence(req, &visibility_checker))?;
        Ok(())
    }

    fn resolve_dictionary(
        &self,
        db_name: Option<&str>,
        dict_name: &str,
        attr_name: &str,
    ) -> Result<TypeCheckDictionary> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_default_catalog()?;
        let db_name = db_name
            .map(ToString::to_string)
            .unwrap_or_else(|| self.ctx.get_current_database());

        let db = self.block_on(catalog.get_database(&tenant, db_name.as_str()))?;
        let db_id = db.get_db_info().database_id.db_id;
        let req = DictionaryNameIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name.to_string()),
        );
        let reply = self.block_on(catalog.get_dictionary(req))?;
        let dictionary = if let Some(r) = reply {
            r.dictionary_meta
        } else {
            return Err(ErrorCode::UnknownDictionary(format!(
                "Unknown dictionary {dict_name}"
            )));
        };

        let attr_field = dictionary.schema.field_with_name(attr_name)?;
        let attr_type: DataType = (&attr_field.data_type).into();
        let default_value = DefaultExprBinder::try_new(self.ctx.clone())?.get_scalar(attr_field)?;

        let primary_column_id = dictionary.primary_column_ids[0];
        let primary_field = dictionary.schema.field_of_column_id(primary_column_id)?;
        let primary_type: DataType = (&primary_field.data_type).into();

        let dict_source = match dictionary.source.as_str() {
            "mysql" => {
                let connection_url = dictionary.build_sql_connection_url()?;
                let table = dictionary
                    .options
                    .get("table")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `table`"))?;
                DictionarySource::Mysql(SqlSource {
                    connection_url,
                    table: table.to_string(),
                    key_field: primary_field.name.clone(),
                    value_field: attr_field.name.clone(),
                })
            }
            "redis" => {
                let host = dictionary
                    .options
                    .get("host")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `host`"))?;
                let port_str = dictionary
                    .options
                    .get("port")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `port`"))?;
                let port = port_str
                    .parse()
                    .expect("Failed to parse String port to u16");
                let username = dictionary.options.get("username").cloned();
                let password = dictionary.options.get("password").cloned();
                let db_index = dictionary
                    .options
                    .get("db_index")
                    .map(|i| i.parse::<i64>().unwrap());
                DictionarySource::Redis(RedisSource {
                    host: host.to_string(),
                    port,
                    username,
                    password,
                    db_index,
                })
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported source {}",
                    dictionary.source
                )));
            }
        };

        Ok(TypeCheckDictionary {
            db_name,
            attr_type,
            primary_type,
            func_arg: DictGetFunctionArgument {
                dict_source,
                default_value,
            },
        })
    }

    fn resolve_read_file_stage_info(&self, span: Span, stage_name: &str) -> Result<StageInfo> {
        self.block_on(async move {
            let (stage_info, _) = StageResolver::from_table_context(
                self.ctx.clone(),
                self.dependencies.user_api_provider.clone(),
                self.dependencies.storage_allow_insecure,
            )?
            .resolve_stage_location(stage_name, StagePathAccess::Read)
            .await?;
            if self
                .ctx
                .get_settings()
                .get_enable_experimental_rbac_check()?
            {
                let visibility_checker = self
                    .ctx
                    .get_visibility_checker(false, Object::Stage)
                    .await?;
                if !(stage_info.is_temporary
                    || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                    || stage_info.stage_type == StageType::User
                        && stage_info.stage_name == self.ctx.get_current_user()?.name)
                {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege READ is required on stage {} for user {}",
                        stage_info.stage_name.clone(),
                        &self.ctx.get_current_user()?.identity().display(),
                    ))
                    .set_span(span));
                }
            }
            Ok(stage_info)
        })
    }

    fn bind_subquery(
        &self,
        parent_context: &BindContext,
        name_resolution_ctx: &NameResolutionContext,
        metadata: MetadataRef,
        subquery: &Query,
    ) -> Result<TypeCheckSubqueryPlan> {
        self.bind_subquery(parent_context, name_resolution_ctx, metadata, subquery)
    }

    fn resolve_namespace_function(&self, function: NamespaceFunction) -> Result<Scalar> {
        let table_access: &dyn TableContextTableAccess = self.ctx.as_ref();
        match function {
            NamespaceFunction::CurrentCatalog => {
                Ok(Scalar::String(table_access.get_current_catalog()))
            }
            NamespaceFunction::CurrentDatabase => {
                Ok(Scalar::String(table_access.get_current_database()))
            }
        }
    }

    fn resolve_session_function(&self, function: SessionFunction<'_>) -> Result<Scalar> {
        match function {
            SessionFunction::Version => Ok(Scalar::String(self.ctx.get_fuse_version())),
            SessionFunction::ConnectionId => Ok(Scalar::String(self.ctx.get_connection_id())),
            SessionFunction::ClientSessionId => Ok(Scalar::String(
                self.ctx.get_current_client_session_id().unwrap_or_default(),
            )),
            SessionFunction::LastQueryId(index) => Ok(self
                .ctx
                .get_last_query_id(index)
                .map(Scalar::String)
                .unwrap_or(Scalar::Null)),
            SessionFunction::Variable(key) => {
                Ok(self.ctx.get_variable(key).unwrap_or(Scalar::Null))
            }
        }
    }

    fn resolve_authorization_function(&self, function: AuthFunction) -> Result<Scalar> {
        let authorization: &dyn TableContextAuthorization = self.ctx.as_ref();
        match function {
            AuthFunction::CurrentUser => Ok(Scalar::String(
                authorization
                    .get_current_user()?
                    .identity()
                    .display()
                    .to_string(),
            )),
            AuthFunction::CurrentRole => Ok(Scalar::String(
                authorization
                    .get_current_role()
                    .map(|role| role.name)
                    .unwrap_or_default(),
            )),
            AuthFunction::CurrentSecondaryRoles => {
                let mut roles = self
                    .block_on(authorization.get_all_effective_roles())?
                    .into_iter()
                    .map(|role| role.name)
                    .collect::<Vec<_>>();
                roles.sort();
                let roles_comma_separated_string = roles.iter().join(",");
                let value = if authorization.get_secondary_roles().is_none() {
                    json!({ "roles": roles_comma_separated_string, "value": "ALL" })
                } else {
                    json!({ "roles": roles_comma_separated_string, "value": "None" })
                };
                Ok(Scalar::String(to_string(&value)?))
            }
            AuthFunction::CurrentAvailableRoles => {
                let mut roles = self
                    .block_on(authorization.get_all_available_roles())?
                    .into_iter()
                    .map(|role| role.name)
                    .collect::<Vec<_>>();
                roles.sort();
                Ok(Scalar::String(to_string(&roles)?))
            }
        }
    }

    fn resolve_effective_role_names(&self) -> Result<Vec<String>> {
        let authorization: &dyn TableContextAuthorization = self.ctx.as_ref();
        Ok(self
            .block_on(authorization.get_all_effective_roles())?
            .into_iter()
            .map(|role| role.name)
            .collect())
    }

    fn set_result_cache_uncacheable(&self) {
        self.ctx.result_cache_state().set_cacheable(false);
    }

    fn resolve_data_mask_policy(
        &self,
        policy_id: u64,
    ) -> Result<Option<Arc<CachedSecurityPolicy>>> {
        if self
            .dependencies
            .license_manager
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)
            .is_err()
        {
            return Ok(None);
        }

        let tenant = self.ctx.get_tenant();
        let meta_api = self.dependencies.user_api_provider.get_meta_store_client();
        let tenant_clone = tenant.clone();
        let policy = self.block_on(self.dependencies.security_policy_cache_manager.get_or_load(
            PolicyType::DataMask,
            &tenant,
            policy_id,
            || async move {
                let handler = get_datamask_handler();
                let seq_v = handler
                    .get_data_mask_by_id(meta_api, &tenant_clone, policy_id)
                    .await?;
                let meta = seq_v.data;
                Ok(RawPolicyDef {
                    body: meta.body,
                    args: meta.args,
                })
            },
        ))?;
        Ok(Some(policy))
    }
}

impl<'a> TypeChecker<'a, FullTypeCheckAdapter> {
    pub fn try_create(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
        forbid_udf: bool,
    ) -> Result<Self> {
        Self::try_create_with_adapter(
            bind_context,
            FullTypeCheckAdapter::new(ctx)?.with_forbid_udf(forbid_udf),
            name_resolution_ctx,
            metadata,
            aliases,
        )
    }

    pub fn try_create_with_alias_fallback(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
        fallback_aliases: Option<&'a [(String, ScalarExpr)]>,
        forbid_udf: bool,
    ) -> Result<Self> {
        Self::try_create_with_adapter_and_alias_fallback(
            bind_context,
            FullTypeCheckAdapter::new(ctx)?.with_forbid_udf(forbid_udf),
            name_resolution_ctx,
            metadata,
            aliases,
            fallback_aliases,
        )
    }
}
