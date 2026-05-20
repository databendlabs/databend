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
use std::time::Duration;

use databend_common_ast::Span;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::pb::CreateWorkerRequest;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ConstantFolder;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::is_builtin_function;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::ScalarUDF;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::init_stage_operator;
use itertools::Itertools;
use serde_json::json;
use serde_json::to_string;
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::CoreUdfCallArgs;
use super::FullTypeCheckAdapter;
use super::StageLocationParam;
use super::TypeChecker;
use super::UdfAdapter;
use super::rewrite_function::rewrite_function_name;
use crate::BindContext;
use crate::ColumnBindingBuilder;
use crate::NameResolutionContext;
use crate::Visibility;
use crate::binder::resolve_file_location;
use crate::binder::resolve_stage_locations;
use crate::binder::wrap_cast;
use crate::planner::expression::UDFValidator;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;
use crate::plans::UDAFCall;
use crate::plans::UDFCall;
use crate::plans::UDFField;
use crate::plans::UDFLambdaCall;
use crate::plans::UDFLanguage;
use crate::plans::UDFScriptCode;
use crate::plans::UDFType;

#[derive(Debug, Clone)]
struct UdfAsset {
    location: String,
    url: String,
    headers: BTreeMap<String, String>,
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn try_lower_udf_call(
        &mut self,
        span: Span,
        func_name: &str,
        func: &'a ASTFunctionCall,
    ) -> Result<Option<CoreExprId>> {
        if is_builtin_function(func_name)
            || TypeChecker::<()>::all_special_functions().contains(&Ascii::new(func_name))
            || rewrite_function_name(func_name).is_some()
        {
            return Ok(None);
        }

        let args = func
            .args
            .iter()
            .map(|arg| Ok::<_, ErrorCode>((format!("{}", arg), self.lower_ast_expr(arg)?)))
            .collect::<Result<CoreUdfCallArgs>>()?;
        Ok(Some(self.alloc(CoreExpr::UdfCall {
            span,
            name: &func.name,
            args,
        })))
    }
}

// UDF server expects unsigned types in UINT* form instead of SQL unsigned names.
fn udf_type_string(data_type: &DataType) -> String {
    let sql_name = data_type.sql_name();
    match sql_name.as_str() {
        "TINYINT UNSIGNED" => "UINT8".to_string(),
        "SMALLINT UNSIGNED" => "UINT16".to_string(),
        "INT UNSIGNED" => "UINT32".to_string(),
        "BIGINT UNSIGNED" => "UINT64".to_string(),
        _ => sql_name,
    }
}

fn build_udf_cloud_script(
    code: &str,
    handler: &str,
    imports: &[UdfAsset],
    packages: &[String],
    input_types: &[String],
    result_type: &str,
) -> Result<String> {
    const UDF_CLOUD_SCRIPT_TEMPLATE: &str = include_str!("udf_cloud_script.sh.tpl");
    const UDF_IMPORTS_BLOCK_TEMPLATE: &str = include_str!("udf_cloud_imports_block.sh.tpl");
    const UDF_IMPORT_DOWNLOADER_TEMPLATE: &str = include_str!("udf_import_downloader.sh.tpl");
    let server_stub = build_udf_cloud_server_stub(handler, input_types, result_type)?;
    let imports_json = if imports.is_empty() {
        None
    } else {
        let imports_payload = imports
            .iter()
            .map(|asset| {
                json!({
                    "location": asset.location.as_str(),
                    "url": asset.url.as_str(),
                    "headers": &asset.headers,
                })
            })
            .collect::<Vec<_>>();
        Some(to_string(&imports_payload)?)
    };
    let mut contents = vec![code, server_stub.as_str()];
    if let Some(json) = imports_json.as_ref() {
        contents.push(json);
    }
    let code_marker = unique_heredoc_marker("UDF_CODE", &contents);
    let server_marker = unique_heredoc_marker("UDF_SERVER", &contents);
    let imports_marker = imports_json
        .as_ref()
        .map(|_| unique_heredoc_marker("UDF_IMPORTS", &contents));

    let packages = packages
        .iter()
        .map(|pkg| pkg.trim())
        .filter(|pkg| !pkg.is_empty())
        .collect::<Vec<_>>();

    let packages_block = if packages.is_empty() {
        String::new()
    } else {
        let mut block = String::from("python -m pip install --no-cache-dir");
        for pkg in &packages {
            block.push(' ');
            block.push_str(pkg);
        }
        block
    };

    let imports_block = if let Some(json) = imports_json {
        let marker = imports_marker.expect("imports marker must exist");
        let mut replacements = BTreeMap::new();
        replacements.insert("IMPORTS_MARKER", marker);
        replacements.insert("IMPORTS_JSON", json);
        replacements.insert(
            "IMPORTS_DOWNLOADER",
            UDF_IMPORT_DOWNLOADER_TEMPLATE.to_string(),
        );
        render_template(UDF_IMPORTS_BLOCK_TEMPLATE, &replacements)?
    } else {
        String::new()
    };

    let mut replacements = BTreeMap::new();
    replacements.insert("PACKAGES_BLOCK", packages_block);
    replacements.insert("IMPORTS_BLOCK", imports_block);
    replacements.insert("CODE_MARKER", code_marker);
    replacements.insert("UDF_CODE", code.to_string());
    replacements.insert("SERVER_MARKER", server_marker);
    replacements.insert("SERVER_STUB", server_stub);
    render_template(UDF_CLOUD_SCRIPT_TEMPLATE, &replacements)
}

fn build_udf_cloud_server_stub(
    handler: &str,
    input_types: &[String],
    result_type: &str,
) -> Result<String> {
    const UDF_CLOUD_SERVER_STUB_TEMPLATE: &str = include_str!("udf_cloud_server_stub.py.tpl");
    let handler_literal = escape_python_double_quoted(handler);
    let input_types_literal = python_string_list(input_types);
    let result_type_literal = escape_python_double_quoted(result_type);
    let mut replacements = BTreeMap::new();
    replacements.insert("HANDLER_LITERAL", handler_literal);
    replacements.insert("INPUT_TYPES_LITERAL", input_types_literal);
    replacements.insert("RESULT_TYPE_LITERAL", result_type_literal);
    render_template(UDF_CLOUD_SERVER_STUB_TEMPLATE, &replacements)
}

fn render_template(template: &str, replacements: &BTreeMap<&str, String>) -> Result<String> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        if let Some(end) = after_start.find("}}") {
            let key = &after_start[..end];
            let value = replacements.get(key).ok_or_else(|| {
                ErrorCode::Internal(format!("missing template placeholder '{key}'"))
            })?;
            output.push_str(value);
            rest = &after_start[end + 2..];
        } else {
            output.push_str(rest);
            return Ok(output);
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn python_string_list(values: &[String]) -> String {
    let items = values
        .iter()
        .map(|value| format!("\"{}\"", escape_python_double_quoted(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{items}]")
}

fn escape_python_double_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn extract_script_metadata_deps(script: &str) -> Vec<String> {
    let mut ss = String::new();
    let mut meta_start = false;
    for line in script.lines() {
        if meta_start {
            if line.starts_with("# ///") {
                break;
            }
            ss.push_str(line.trim_start_matches('#').trim());
            ss.push('\n');
        }
        if !meta_start && line.starts_with("# /// script") {
            meta_start = true;
        }
    }

    let parsed = ss.parse::<toml::Value>().unwrap();

    if parsed.get("dependencies").is_none() {
        return Vec::new();
    }

    if let Some(deps) = parsed["dependencies"].as_array() {
        deps.iter()
            .filter_map(|value| value.as_str().map(|item| item.to_string()))
            .collect()
    } else {
        Vec::new()
    }
}

fn unique_heredoc_marker(base: &str, contents: &[&str]) -> String {
    let mut suffix = 0;
    let mut marker = base.to_string();
    while contents.iter().any(|content| content.contains(&marker)) {
        suffix += 1;
        marker = format!("{base}_{suffix}");
    }
    marker
}

impl FullTypeCheckAdapter {
    fn build_udf_cloud_imports(&self, imports: &[String]) -> Result<Vec<UdfAsset>> {
        if imports.is_empty() {
            return Ok(Vec::new());
        }

        let stage_locations = self.block_on(resolve_stage_locations(self.ctx.as_ref(), imports))?;
        let expire = Duration::from_secs(
            self.ctx
                .get_settings()
                .get_udf_cloud_import_presign_expire_secs()?,
        );

        self.block_on(async move {
            let mut results = Vec::with_capacity(stage_locations.len());
            for ((stage_info, path), location) in stage_locations.into_iter().zip(imports.iter()) {
                let op = init_stage_operator(&stage_info).map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to get StageTable operator for UDF import '{}': {}",
                        location, err
                    ))
                })?;
                if !op.info().full_capability().presign {
                    return Err(ErrorCode::StorageUnsupported(
                        "storage doesn't support presign operation",
                    ));
                }
                op.stat(&path).await.map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to stat UDF import '{}': {}",
                        location, err
                    ))
                })?;
                let presigned = op.presign_read(&path, expire).await.map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to presign UDF import '{}': {}",
                        location, err
                    ))
                })?;
                let headers = presigned
                    .header()
                    .into_iter()
                    .map(|(name, value)| {
                        let value = value.to_str().map_err(|err| {
                            ErrorCode::SemanticError(format!(
                                "Failed to parse presigned header for UDF import '{}': {err}",
                                location
                            ))
                        })?;
                        Ok((name.as_str().to_string(), value.to_string()))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                results.push(UdfAsset {
                    location: location.to_string(),
                    url: presigned.uri().to_string(),
                    headers,
                });
            }

            Ok(results)
        })
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

        let resp = self.block_on(
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
}

impl UdfAdapter for FullTypeCheckAdapter {
    fn load_definition(&self, udf_name: &str) -> Result<Option<UserDefinedFunction>> {
        let tenant = self.ctx.get_tenant();
        self.block_on(async {
            self.dependencies
                .user_api_provider
                .get_udf(&tenant, udf_name)
                .await
                .map_err(ErrorCode::from)
        })
    }

    fn load_stage_locations(&self, locations: &[String]) -> Result<Vec<(StageInfo, String)>> {
        self.block_on(resolve_stage_locations(self.ctx.as_ref(), locations))
    }

    fn load_udf_code(&self, code: String) -> Result<Vec<u8>> {
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

        let (stage_info, module_path) = self.block_on(async {
            resolve_file_location(self.ctx.as_ref(), &file_location)
                .await
                .map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to resolve code location {code:?}: {err}"
                    ))
                })
        })?;

        let op = init_stage_operator(&stage_info).map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to get StageTable operator: {err}"))
        })?;

        let code_blob = self
            .block_on(async {
                op.read(&module_path).await.map_err(|err| {
                    ErrorCode::SemanticError(format!("Failed to read module {module_path}: {err}"))
                })
            })?
            .to_vec();

        let compress_algo = CompressAlgorithm::from_path(&module_path);
        log::trace!("Detecting compression algorithm for module: {module_path}");
        log::info!("Detected compression algorithm: {compress_algo:#?}");

        let code_blob = match compress_algo {
            Some(algo) => {
                log::trace!("Decompressing module using {algo:?} algorithm");
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
                    let error_msg = format!("Failed to decompress module {module_path}: {err}");
                    log::error!("{error_msg}");
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
        self.block_on(async move {
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
            Ok::<_, ErrorCode>(value.to_owned())
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

    fn apply_udf_cloud_script(
        &self,
        resource_name: &str,
        udf_definition: UDFScript,
    ) -> Result<UDFServer> {
        let UDFScript {
            code,
            imports,
            packages,
            handler,
            language,
            arg_types,
            return_type,
            runtime_version: _,
            immutable,
        } = udf_definition;
        let code_bytes = self.load_udf_code(code)?;
        let resolved_code = String::from_utf8(code_bytes).map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to parse UDF code as utf-8: {err}"))
        })?;
        let import_assets = self.build_udf_cloud_imports(&imports)?;
        let mut merged_packages = extract_script_metadata_deps(&resolved_code);
        merged_packages.extend_from_slice(&packages);
        let input_types = arg_types.iter().map(udf_type_string).collect::<Vec<_>>();
        let result_type = udf_type_string(&return_type);
        let script = build_udf_cloud_script(
            &resolved_code,
            &handler,
            &import_assets,
            &merged_packages,
            &input_types,
            &result_type,
        )?;
        let (endpoint, headers) = self.apply_udf_cloud_resource(resource_name, "udf", script)?;
        Ok(UDFServer {
            address: endpoint,
            handler,
            headers,
            language,
            arg_names: Vec::new(),
            arg_types,
            return_type,
            immutable,
        })
    }
}

struct UdfCallResolver<A: UdfAdapter> {
    udf_adapter: A,
    func_ctx: FunctionContext,
}

struct UdfArgument {
    display_name: String,
    scalar: ScalarExpr,
    data_type: DataType,
}

enum UdfResolveResult {
    Expr(Box<(ScalarExpr, DataType)>),
    RuntimeServerExpr(Box<(ScalarExpr, DataType)>),
    RuntimeScriptExpr(Box<(ScalarExpr, DataType)>),
    LambdaDefinition {
        span: Span,
        func_name: String,
        definition: String,
        parameters: Vec<(String, DataType, ScalarExpr)>,
    },
    ScalarDefinition {
        span: Span,
        func_name: String,
        definition: String,
        parameters: Vec<(String, DataType, ScalarExpr)>,
        return_type: DataType,
    },
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    pub(super) fn resolve_udf_call(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        name: &Identifier,
        args: &CoreUdfCallArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let udf_name = normalize_identifier(name, self.name_resolution_ctx).to_string();
        if self.adapter.forbid_udf() {
            return Err(self.unknown_function_error(span, &udf_name));
        }

        let udf = self.resolve_udf_from_cache(&udf_name)?;
        let Some(udf) = udf else {
            return Err(self.unknown_function_error(span, &udf_name));
        };

        let arguments = args
            .iter()
            .map(|(display_name, arg)| {
                let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
                Ok(UdfArgument {
                    display_name: display_name.clone(),
                    scalar,
                    data_type,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let udf = {
            let mut udf_resolver = UdfCallResolver {
                udf_adapter: self.adapter.udf_adapter(),
                func_ctx: self.func_ctx.clone(),
            };
            udf_resolver.resolve_udf(span, udf, &arguments)?
        };
        match udf {
            UdfResolveResult::Expr(expr) => Ok(expr),
            UdfResolveResult::RuntimeServerExpr(expr) => {
                self.bind_context.have_udf_server = true;
                self.adapter.set_result_cache_uncacheable();
                Ok(expr)
            }
            UdfResolveResult::RuntimeScriptExpr(expr) => {
                self.bind_context.have_udf_script = true;
                self.adapter.set_result_cache_uncacheable();
                Ok(expr)
            }
            UdfResolveResult::LambdaDefinition {
                span,
                func_name,
                definition,
                parameters,
            } => self.resolve_lambda_udf_definition(span, func_name, &definition, parameters),
            UdfResolveResult::ScalarDefinition {
                span,
                func_name,
                definition,
                parameters,
                return_type,
            } => self.resolve_scalar_udf_definition(
                span,
                func_name,
                &definition,
                parameters,
                return_type,
            ),
        }
    }

    fn resolve_udf_from_cache(&mut self, udf_name: &str) -> Result<Option<UserDefinedFunction>> {
        if let Some(udf) = self.bind_context.udf_cache.read().get(udf_name).cloned() {
            return Ok(udf);
        }

        let udf = self.adapter.udf_adapter().load_definition(udf_name)?;
        self.bind_context
            .udf_cache
            .write()
            .insert(udf_name.to_string(), udf.clone());
        Ok(udf)
    }

    fn resolve_udf_definition(
        &mut self,
        definition: &str,
        parameters: Vec<(String, DataType, ScalarExpr)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let sql_tokens = tokenize_sql(definition)?;
        let expr = parse_expr(&sql_tokens, self.adapter.settings().get_sql_dialect()?)?;

        let mut bind_context = BindContext::with_parent(self.bind_context.clone())?;
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
            ..self.name_resolution_ctx.clone()
        };
        let box (mut scalar, data_type) = TypeChecker::try_create_with_adapter(
            &mut bind_context,
            self.adapter.clone(),
            &name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
        )?
        .resolve(&expr)?;

        for (column_index, arg) in replacements {
            scalar.replace_column_with_scalar(column_index, &arg)?;
        }

        Ok(Box::new((scalar, data_type)))
    }

    fn resolve_lambda_udf_definition(
        &mut self,
        span: Span,
        func_name: String,
        definition: &str,
        parameters: Vec<(String, DataType, ScalarExpr)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (scalar, data_type) = self.resolve_udf_definition(definition, parameters)?;
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(scalar),
            }
            .into(),
            data_type,
        )))
    }

    fn resolve_scalar_udf_definition(
        &mut self,
        span: Span,
        func_name: String,
        definition: &str,
        parameters: Vec<(String, DataType, ScalarExpr)>,
        return_type: DataType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (expr, _) = self.resolve_udf_definition(definition, parameters)?;
        let expr = CastExpr {
            span,
            is_try: false,
            argument: Box::new(expr),
            target_type: Box::new(return_type.clone()),
        };
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(expr.into()),
            }
            .into(),
            return_type,
        )))
    }
}

impl<A> UdfCallResolver<A>
where A: UdfAdapter
{
    fn resolve_udf(
        &mut self,
        span: Span,
        udf: UserDefinedFunction,
        arguments: &[UdfArgument],
    ) -> Result<UdfResolveResult> {
        let name = udf.name;

        match udf.definition {
            UDFDefinition::LambdaUDF(udf_def) => {
                self.resolve_lambda_udf(span, name, arguments, udf_def)
            }
            UDFDefinition::UDFServer(udf_def) => {
                self.resolve_udf_server(span, name, arguments, udf_def, true)
            }
            UDFDefinition::UDFScript(udf_def) => {
                self.resolve_udf_script(span, name, arguments, udf_def)
            }
            UDFDefinition::UDAFScript(udf_def) => {
                self.resolve_udaf_script(span, name, arguments, udf_def)
            }
            UDFDefinition::UDTF(_) => unreachable!(),
            UDFDefinition::UDTFServer(_) => unreachable!(),
            UDFDefinition::ScalarUDF(udf_def) => {
                self.resolve_scalar_udf(span, name, arguments, udf_def)
            }
        }
    }

    fn resolve_udf_server(
        &mut self,
        span: Span,
        name: String,
        arguments: &[UdfArgument],
        mut udf_definition: UDFServer,
        validate_address: bool,
    ) -> Result<UdfResolveResult> {
        if validate_address {
            UDFValidator::is_udf_server_allowed(&udf_definition.address)?;
        }
        if arguments.len() != udf_definition.arg_types.len() {
            return Err(ErrorCode::InvalidArgument(format!(
                "Require {} parameters, but got: {}",
                udf_definition.arg_types.len(),
                arguments.len()
            ))
            .set_span(span));
        }

        let mut all_args_const = true;
        let mut args = Vec::with_capacity(arguments.len());
        let mut stage_locations = Vec::new();
        for (i, (argument, dest_type)) in arguments
            .iter()
            .zip(udf_definition.arg_types.iter())
            .enumerate()
        {
            // TODO: support cast constant
            if !matches!(argument.scalar, ScalarExpr::ConstantExpr(_))
                || (argument.data_type != dest_type.remove_nullable()
                    && dest_type.remove_nullable() != DataType::StageLocation)
            {
                all_args_const = false;
            }
            if dest_type.remove_nullable() == DataType::StageLocation {
                if udf_definition.arg_names.is_empty() {
                    return Err(ErrorCode::InvalidArgument(
                        "StageLocation must have a corresponding variable name",
                    ));
                }
                let expr = argument.scalar.as_expr()?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let Ok(Some(location)) =
                    expr.into_constant().map(|c| c.scalar.as_string().cloned())
                else {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid parameter {argument} for udf function, expected constant string",
                        argument = argument.display_name
                    ))
                    .set_span(span));
                };
                let mut resolved = self
                    .udf_adapter
                    .load_stage_locations(std::slice::from_ref(&location))?;
                let (stage_info, relative_path) = resolved.pop().unwrap();

                if !matches!(stage_info.stage_type, StageType::External) {
                    return Err(ErrorCode::SemanticError(format!(
                        "stage {} type is {}, UDF only support External Stage",
                        stage_info.stage_name, stage_info.stage_type,
                    ))
                    .set_span(span));
                }
                if let StorageParams::S3(config) = &stage_info.stage_params.storage {
                    if !config.security_token.is_empty() || !config.role_arn.is_empty() {
                        return Err(ErrorCode::SemanticError(format!(
                            "StageLocation: @{} must use a separate credential",
                            location
                        )));
                    }
                }

                stage_locations.push(StageLocationParam {
                    param_name: udf_definition.arg_names[i].clone(),
                    relative_path,
                    stage_info,
                });
                continue;
            }
            if argument.data_type != *dest_type {
                args.push(wrap_cast(&argument.scalar, dest_type));
            } else {
                args.push(argument.scalar.clone());
            }
        }
        if !stage_locations.is_empty() {
            let stage_location_value = serde_json::to_string(&stage_locations)?;
            udf_definition
                .headers
                .insert("databend-stage-mapping".to_string(), stage_location_value);
        }
        let immutable = udf_definition.immutable.unwrap_or_default();
        if immutable && all_args_const {
            let mut arg_scalars = Vec::with_capacity(args.len());
            for arg in &args {
                let arg_scalar = match arg {
                    ScalarExpr::ConstantExpr(constant) => constant.value.clone(),
                    ScalarExpr::CastExpr(cast) => {
                        let constant = ConstantExpr::try_from(*cast.argument.clone()).unwrap();
                        constant.value.clone()
                    }
                    _ => unreachable!(),
                };
                arg_scalars.push(arg_scalar);
            }
            let value = self.udf_adapter.fold_udf_server(
                name.as_str(),
                arg_scalars,
                udf_definition.clone(),
            )?;
            return Ok(UdfResolveResult::Expr(Box::new((
                ConstantExpr { span, value }.into(),
                udf_definition.return_type.clone(),
            ))));
        }

        let arg_names = arguments
            .iter()
            .map(|arg| arg.display_name.as_str())
            .join(", ");
        let display_name = format!("{}({})", udf_definition.handler, arg_names);

        Ok(UdfResolveResult::RuntimeServerExpr(Box::new((
            UDFCall {
                span,
                name,
                handler: udf_definition.handler,
                headers: udf_definition.headers,
                display_name,
                udf_type: UDFType::Server(udf_definition.address.clone()),
                arg_types: udf_definition.arg_types,
                return_type: Box::new(udf_definition.return_type.clone()),
                arguments: args,
            }
            .into(),
            udf_definition.return_type.clone(),
        ))))
    }

    fn resolve_udf_script(
        &mut self,
        span: Span,
        name: String,
        arguments: &[UdfArgument],
        udf_definition: UDFScript,
    ) -> Result<UdfResolveResult> {
        let language = udf_definition.language.parse()?;
        let use_cloud =
            matches!(language, UDFLanguage::Python) && self.udf_adapter.enable_udf_sandbox()?;
        if use_cloud {
            UDFValidator::is_udf_cloud_script_allowed(&language)?;
            let udf_definition = self
                .udf_adapter
                .apply_udf_cloud_script(&name, udf_definition)?;
            return self.resolve_udf_server(span, name, arguments, udf_definition, false);
        }

        UDFValidator::is_udf_script_allowed(&language)?;
        let UDFScript {
            code,
            handler,
            language: _,
            arg_types,
            return_type,
            runtime_version,
            imports,
            packages,
            immutable: _,
        } = udf_definition;
        let mut scalar_arguments = Vec::with_capacity(arguments.len());
        for (argument, dest_type) in arguments.iter().zip(arg_types.iter()) {
            if argument.data_type != *dest_type {
                scalar_arguments.push(wrap_cast(&argument.scalar, dest_type));
            } else {
                scalar_arguments.push(argument.scalar.clone());
            }
        }

        let code_blob = self.udf_adapter.load_udf_code(code)?.into_boxed_slice();
        let imports_stage_info = self.udf_adapter.load_stage_locations(&imports)?;

        let udf_type = UDFType::Script(Box::new(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
            imports_stage_info,
            imports,
            packages,
        }));

        let arg_names = arguments
            .iter()
            .map(|arg| arg.display_name.as_str())
            .join(", ");
        let display_name = format!("{}({})", &handler, arg_names);

        Ok(UdfResolveResult::RuntimeScriptExpr(Box::new((
            UDFCall {
                span,
                name,
                handler,
                headers: BTreeMap::default(),
                display_name,
                arg_types,
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments: scalar_arguments,
            }
            .into(),
            return_type,
        ))))
    }

    fn resolve_udaf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[UdfArgument],
        udf_definition: UDAFScript,
    ) -> Result<UdfResolveResult> {
        let UDAFScript {
            code,
            language,
            arg_types,
            state_fields,
            return_type,
            runtime_version,
            imports,
            packages,
        } = udf_definition;
        let language = language.parse()?;
        let code_blob = self.udf_adapter.load_udf_code(code)?.into_boxed_slice();
        let imports_stage_info = self.udf_adapter.load_stage_locations(&imports)?;

        let udf_type = UDFType::Script(Box::new(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
            imports,
            imports_stage_info,
            packages,
        }));

        let arguments = args
            .iter()
            .zip(arg_types.iter())
            .map(|(argument, dest_type)| {
                Ok(if argument.data_type == *dest_type {
                    argument.scalar.clone()
                } else {
                    wrap_cast(&argument.scalar, dest_type)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let display_name = format!(
            "{name}({})",
            args.iter().map(|arg| arg.display_name.as_str()).join(", ")
        );

        Ok(UdfResolveResult::RuntimeScriptExpr(Box::new((
            UDAFCall {
                span,
                name,
                display_name,
                arg_types,
                state_fields: state_fields
                    .iter()
                    .map(|f| UDFField {
                        name: f.name().to_string(),
                        data_type: f.data_type().clone(),
                    })
                    .collect(),
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments,
            }
            .into(),
            return_type,
        ))))
    }

    fn resolve_lambda_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[UdfArgument],
        udf_definition: LambdaUDF,
    ) -> Result<UdfResolveResult> {
        let parameters = udf_definition.parameters;
        if parameters.len() != arguments.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Require {} parameters, but got: {}",
                parameters.len(),
                arguments.len()
            ))
            .set_span(span));
        }
        let mut resolved_arguments = Vec::with_capacity(arguments.len());
        let mut arg_types = Vec::with_capacity(arguments.len());
        for argument in arguments {
            resolved_arguments.push(argument.scalar.clone());
            arg_types.push(argument.data_type.clone());
        }
        let parameters = parameters
            .into_iter()
            .zip(arg_types)
            .zip(resolved_arguments)
            .map(|((name, data_type), scalar)| (name, data_type, scalar))
            .collect();
        Ok(UdfResolveResult::LambdaDefinition {
            span,
            func_name,
            definition: udf_definition.definition,
            parameters,
        })
    }

    fn resolve_scalar_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[UdfArgument],
        udf_definition: ScalarUDF,
    ) -> Result<UdfResolveResult> {
        let arg_types = udf_definition.arg_types;
        if arg_types.len() != arguments.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Require {} parameters, but got: {}",
                arg_types.len(),
                arguments.len()
            ))
            .set_span(span));
        }
        let mut parameters = Vec::with_capacity(arg_types.len());
        for ((arg_name, dest_type), argument) in arg_types.iter().zip(arguments.iter()) {
            let arg = if argument.data_type != *dest_type {
                wrap_cast(&argument.scalar, dest_type)
            } else {
                argument.scalar.clone()
            };
            parameters.push((arg_name.clone(), dest_type.clone(), arg));
        }
        Ok(UdfResolveResult::ScalarDefinition {
            span,
            func_name,
            definition: udf_definition.definition,
            parameters,
            return_type: udf_definition.return_type,
        })
    }
}
