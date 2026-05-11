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

use databend_common_ast::Span;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::block_on_with_handle;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::CreateWorkerRequest;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_settings::Settings;
use databend_common_storage::init_stage_operator;
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
use tokio::runtime::Handle;

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
use crate::ColumnBindingBuilder;
use crate::DefaultExprBinder;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::Visibility;
use crate::binder::resolve_file_location;
use crate::binder::resolve_stage_location;
use crate::binder::resolve_stage_locations;
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

        Self {
            async_runtime_handle: tokio::runtime::Handle::current(),
            aggregate_function_factory: AggregateFunctionFactory::instance(),
            license_manager: LicenseManagerSwitch::instance(),
            catalog_manager: CatalogManager::instance(),
            user_api_provider: UserApiProvider::instance(),
            security_policy_cache_manager: SecurityPolicyCacheManager::instance(),
            global_config,
            cloud_control_api_provider,
        }
    }
}

impl TypeCheckAdapter for FullTypeCheckAdapter {
    fn function_context(&self) -> Result<FunctionContext> {
        self.ctx.get_function_context()
    }

    fn settings(&self) -> Arc<Settings> {
        self.ctx.get_settings()
    }

    fn aggregate_function_factory(&self) -> &'static AggregateFunctionFactory {
        self.dependencies.aggregate_function_factory
    }

    fn forbid_udf(&self) -> bool {
        self.forbid_udf
    }

    fn async_runtime_handle(&self) -> Result<Handle> {
        Ok(self.dependencies.async_runtime_handle.clone())
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
            Some(block_on_with_handle(
                &self.dependencies.async_runtime_handle,
                async move { ctx.get_visibility_checker(false, Object::Sequence).await },
            )?)
        } else {
            None
        };
        let _ = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            catalog.get_sequence(req, &visibility_checker),
        )?;
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

        let db = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            catalog.get_database(&tenant, db_name.as_str()),
        )?;
        let db_id = db.get_db_info().database_id.db_id;
        let req = DictionaryNameIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name.to_string()),
        );
        let reply = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            catalog.get_dictionary(req),
        )?;
        let dictionary = if let Some(r) = reply {
            r.dictionary_meta
        } else {
            return Err(ErrorCode::UnknownDictionary(format!(
                "Unknown dictionary {}",
                dict_name,
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
        block_on_with_handle(&self.dependencies.async_runtime_handle, async move {
            let (stage_info, _) = resolve_stage_location(self.ctx.as_ref(), stage_name).await?;
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
                let mut roles = block_on_with_handle(
                    &self.dependencies.async_runtime_handle,
                    authorization.get_all_effective_roles(),
                )?
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
                let mut roles = block_on_with_handle(
                    &self.dependencies.async_runtime_handle,
                    authorization.get_all_available_roles(),
                )?
                .into_iter()
                .map(|role| role.name)
                .collect::<Vec<_>>();
                roles.sort();
                Ok(Scalar::String(to_string(&roles)?))
            }
        }
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
        let policy = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            self.dependencies.security_policy_cache_manager.get_or_load(
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
            ),
        )?;
        Ok(Some(policy))
    }

    fn resolve_udf(&self, udf_name: &str) -> Result<Option<UserDefinedFunction>> {
        let tenant = self.ctx.get_tenant();
        Ok(block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            self.dependencies
                .user_api_provider
                .get_udf(&tenant, udf_name),
        )?)
    }

    fn resolve_udf_stage_locations(
        &self,
        locations: &[String],
    ) -> Result<Vec<(StageInfo, String)>> {
        block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            resolve_stage_locations(self.ctx.as_ref(), locations),
        )
    }

    fn resolve_udf_code(&self, code: String) -> Result<Vec<u8>> {
        let file_location = match code.strip_prefix('@') {
            Some(location) => FileLocation::Stage(location.to_string()),
            None => {
                let uri = UriLocation::from_uri(code.clone(), BTreeMap::default());

                match uri {
                    Ok(uri) => FileLocation::Uri(uri),
                    Err(_) => {
                        return Ok(code.into());
                    }
                }
            }
        };

        let (stage_info, module_path) = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            resolve_file_location(self.ctx.as_ref(), &file_location),
        )
        .map_err(|err| {
            ErrorCode::SemanticError(format!(
                "Failed to resolve code location {:?}: {}",
                code, err
            ))
        })?;

        let op = init_stage_operator(&stage_info).map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to get StageTable operator: {}", err))
        })?;

        let code_blob = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            op.read(&module_path),
        )
        .map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to read module {}: {}", module_path, err))
        })?
        .to_vec();

        let compress_algo = CompressAlgorithm::from_path(&module_path);
        log::trace!(
            "Detecting compression algorithm for module: {}",
            &module_path
        );
        log::info!("Detected compression algorithm: {:#?}", &compress_algo);

        let code_blob = match compress_algo {
            Some(algo) => {
                log::trace!("Decompressing module using {:?} algorithm", algo);
                if algo == CompressAlgorithm::Zip {
                    DecompressDecoder::decompress_all_zip(
                        &code_blob,
                        &module_path,
                        GLOBAL_MEM_STAT.get_limit() as usize,
                    )
                } else {
                    let mut decoder = DecompressDecoder::new(algo);
                    decoder.decompress_all(&code_blob)
                }
                .map_err(|err| {
                    let error_msg = format!("Failed to decompress module {}: {}", module_path, err);
                    log::error!("{}", error_msg);
                    ErrorCode::SemanticError(error_msg)
                })?
            }
            None => code_blob,
        };

        Ok(code_blob)
    }

    fn fold_udf_server(
        &self,
        name: &str,
        args: Vec<Scalar>,
        udf_definition: UDFServer,
    ) -> Result<Scalar> {
        let mut block_entries = Vec::with_capacity(args.len());
        for (arg, dest_type) in args.into_iter().zip(
            udf_definition
                .arg_types
                .iter()
                .filter(|ty| ty.remove_nullable() != DataType::StageLocation),
        ) {
            if matches!(dest_type, DataType::StageLocation) {
                continue;
            }
            let entry = BlockEntry::new_const_column(dest_type.clone(), arg, 1);
            block_entries.push(entry);
        }

        let settings = self.ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_timeout = settings.get_external_server_request_timeout_secs()?;

        let handler = udf_definition.handler;
        let return_type = udf_definition.return_type;
        let endpoint = databend_common_expression::udf_client::UDFFlightClient::build_endpoint(
            &udf_definition.address,
            connect_timeout,
            request_timeout,
            &self.ctx.get_version().udf_client_user_agent(),
        )?;

        let tenant_name = self.ctx.get_tenant().tenant_name().to_string();
        let query_id = self.ctx.get_id();
        let name = name.to_string();
        let headers = udf_definition.headers;
        block_on_with_handle(&self.dependencies.async_runtime_handle, async move {
            let num_rows = 1;
            let mut client = databend_common_expression::udf_client::UDFFlightClient::connect(
                &handler,
                endpoint,
                connect_timeout,
                num_rows,
            )
            .await?
            .with_tenant(&tenant_name)?
            .with_func_name(&name)?
            .with_handler_name(&handler)?
            .with_query_id(&query_id)?
            .with_headers(headers)?;

            let result = client
                .do_exchange(&name, &handler, Some(num_rows), block_entries, &return_type)
                .await?;

            let value = unsafe { result.get_by_offset(0).index_unchecked(0) };
            Ok(value.to_owned())
        })
    }

    fn enable_udf_sandbox(&self) -> Result<bool> {
        Ok(self
            .dependencies
            .global_config
            .query
            .common
            .enable_udf_sandbox)
    }

    fn apply_udf_cloud_resource(
        &self,
        resource_name: &str,
        resource_type: &str,
        script: String,
    ) -> Result<(String, BTreeMap<String, String>)> {
        let Some(provider) = self.dependencies.cloud_control_api_provider.clone() else {
            return Err(ErrorCode::Unimplemented(
                "SandboxUDF requires cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        };

        let tenant = self.ctx.get_tenant();
        let user = self
            .ctx
            .get_current_user()?
            .identity()
            .display()
            .to_string();
        let query_id = self.ctx.get_id();
        let mut cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            provider.get_timeout(),
        );
        cfg.add_worker_version_info();

        let req = CreateWorkerRequest {
            tenant_id: tenant.tenant_name().to_string(),
            name: resource_name.to_string(),
            if_not_exists: true,
            tags: Default::default(),
            options: Default::default(),
            r#type: resource_type.to_string(),
            script,
        };

        let resp = block_on_with_handle(
            &self.dependencies.async_runtime_handle,
            provider
                .get_worker_client()
                .create_worker(make_request(req, cfg)),
        )?;

        let endpoint = resp.endpoint;
        if endpoint.is_empty() {
            return Err(ErrorCode::CloudControlConnectError(
                "UDF cloud resource endpoint is empty".to_string(),
            ));
        }

        Ok((endpoint, resp.headers))
    }

    fn resolve_udf_definition(
        &self,
        parent_context: &BindContext,
        name_resolution_ctx: &NameResolutionContext,
        metadata: MetadataRef,
        aliases: &[(String, ScalarExpr)],
        definition: &str,
        parameters: Vec<(String, DataType, ScalarExpr)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let sql_tokens = tokenize_sql(definition)?;
        let expr = parse_expr(&sql_tokens, self.settings().get_sql_dialect()?)?;

        let mut bind_context = BindContext::with_parent(parent_context.clone())?;
        let mut replacements = Vec::with_capacity(parameters.len());
        for (name, data_type, scalar) in parameters {
            let column_index = bind_context.next_column_index();
            bind_context.add_column_binding(
                ColumnBindingBuilder::new(
                    name,
                    column_index,
                    Box::new(data_type),
                    Visibility::Visible,
                )
                .build(),
            );
            replacements.push((column_index, scalar));
        }

        let name_resolution_ctx = NameResolutionContext {
            deny_column_reference: false,
            ..name_resolution_ctx.clone()
        };
        let box (mut scalar, data_type) = TypeChecker::try_create_with_adapter(
            &mut bind_context,
            self.clone(),
            &name_resolution_ctx,
            metadata,
            aliases,
        )?
        .resolve(&expr)?;

        for (column_index, arg) in replacements {
            scalar.replace_column_with_scalar(column_index, &arg)?;
        }

        Ok(Box::new((scalar, data_type)))
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
}
