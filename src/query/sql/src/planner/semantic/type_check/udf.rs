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
use std::collections::HashMap;
use std::time::Duration;

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
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
use databend_common_expression::ConstantFolder;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::ScalarUDF;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::init_stage_operator;
use databend_common_users::UserApiProvider;
use derive_visitor::DriveMut;
use itertools::Itertools;
use serde_json::json;
use serde_json::to_string;

use super::StageLocationParam;
use super::TypeChecker;
use crate::UDFArgVisitor;
use crate::binder::resolve_file_location;
use crate::binder::resolve_stage_location;
use crate::binder::resolve_stage_locations;
use crate::binder::wrap_cast;
use crate::planner::expression::UDFValidator;
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

impl<'a> TypeChecker<'a> {
    pub(super) fn resolve_udf(
        &mut self,
        span: Span,
        udf_name: &str,
        arguments: &[Expr],
    ) -> Result<Option<Box<(ScalarExpr, DataType)>>> {
        if self.forbid_udf {
            return Ok(None);
        }

        let udf = if let Some(udf) = self.bind_context.udf_cache.read().get(udf_name).cloned() {
            udf
        } else {
            let tenant = self.ctx.get_tenant();
            let provider = UserApiProvider::instance();
            let udf = databend_common_base::runtime::block_on(provider.get_udf(&tenant, udf_name))?;
            self.bind_context
                .udf_cache
                .write()
                .insert(udf_name.to_string(), udf.clone());
            udf
        };

        let Some(udf) = udf else {
            return Ok(None);
        };

        let name = udf.name;

        match udf.definition {
            UDFDefinition::LambdaUDF(udf_def) => Ok(Some(
                self.resolve_lambda_udf(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDFServer(udf_def) => Ok(Some(
                self.resolve_udf_server(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDFScript(udf_def) => Ok(Some(
                self.resolve_udf_script(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDAFScript(udf_def) => Ok(Some(
                self.resolve_udaf_script(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDTF(_) => unreachable!(),
            UDFDefinition::UDTFServer(_) => unreachable!(),
            UDFDefinition::ScalarUDF(udf_def) => Ok(Some(
                self.resolve_scalar_udf(span, name, arguments, udf_def)?,
            )),
        }
    }

    fn resolve_udf_server(
        &mut self,
        span: Span,
        name: String,
        arguments: &[Expr],
        udf_definition: UDFServer,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        self.resolve_udf_server_internal(span, name, arguments, udf_definition, true)
    }

    fn resolve_udf_server_internal(
        &mut self,
        span: Span,
        name: String,
        arguments: &[Expr],
        mut udf_definition: UDFServer,
        validate_address: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
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
            let box (arg, ty) = self.resolve(argument)?;
            // TODO: support cast constant
            if !matches!(arg, ScalarExpr::ConstantExpr(_))
                || (ty != dest_type.remove_nullable()
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
                let expr = arg.as_expr()?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let Ok(Some(location)) =
                    expr.into_constant().map(|c| c.scalar.as_string().cloned())
                else {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid parameter {argument} for udf function, expected constant string",
                    ))
                    .set_span(span));
                };
                let (stage_info, relative_path) = databend_common_base::runtime::block_on(
                    resolve_stage_location(self.ctx.as_ref(), &location),
                )?;

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
            if ty != *dest_type {
                args.push(wrap_cast(&arg, dest_type));
            } else {
                args.push(arg);
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
            let value = databend_common_base::runtime::block_on(self.fold_udf_server(
                name.as_str(),
                arg_scalars,
                udf_definition.clone(),
            ))?;
            return Ok(Box::new((
                ConstantExpr { span, value }.into(),
                udf_definition.return_type.clone(),
            )));
        }

        let arg_names = arguments.iter().map(|arg| format!("{arg}")).join(", ");
        let display_name = format!("{}({})", udf_definition.handler, arg_names);

        self.bind_context.have_udf_server = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
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
        )))
    }

    #[async_backtrace::framed]
    pub async fn fold_udf_server(
        &mut self,
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

        let endpoint = UDFFlightClient::build_endpoint(
            &udf_definition.address,
            connect_timeout,
            request_timeout,
            &self.ctx.get_version().udf_client_user_agent(),
        )?;

        let num_rows = 1;
        let mut client =
            UDFFlightClient::connect(&udf_definition.handler, endpoint, connect_timeout, num_rows)
                .await?
                .with_tenant(self.ctx.get_tenant().tenant_name())?
                .with_func_name(name)?
                .with_handler_name(&udf_definition.handler)?
                .with_query_id(&self.ctx.get_id())?
                .with_headers(udf_definition.headers)?;

        let result = client
            .do_exchange(
                name,
                &udf_definition.handler,
                Some(num_rows),
                block_entries,
                &udf_definition.return_type,
            )
            .await?;

        let value = unsafe { result.get_by_offset(0).index_unchecked(0) };
        Ok(value.to_owned())
    }

    fn apply_udf_cloud_resource(
        &self,
        resource_name: &str,
        resource_type: &str,
        script: String,
    ) -> Result<(String, BTreeMap<String, String>)> {
        let Some(_) = &GlobalConfig::instance()
            .query
            .common
            .cloud_control_grpc_server_address
        else {
            return Err(ErrorCode::Unimplemented(
                "SandboxUDF requires cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        };

        let provider = CloudControlApiProvider::instance();
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

        let resp = databend_common_base::runtime::block_on(
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

    fn build_udf_cloud_imports(
        &self,
        imports: &[String],
        expire: Duration,
    ) -> Result<Vec<UdfAsset>> {
        if imports.is_empty() {
            return Ok(Vec::new());
        }

        let locations = imports
            .iter()
            .map(|location| location.trim_start_matches('@').to_string())
            .collect::<Vec<_>>();

        let stage_locations = databend_common_base::runtime::block_on(resolve_stage_locations(
            self.ctx.as_ref(),
            &locations,
        ))?;

        databend_common_base::runtime::block_on(async move {
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

    fn build_udf_cloud_script(
        &self,
        code: &str,
        handler: &str,
        imports: &[UdfAsset],
        packages: &[String],
        arg_types: &[DataType],
        return_type: &DataType,
    ) -> Result<String> {
        let input_types = arg_types.iter().map(udf_type_string).collect::<Vec<_>>();
        let result_type = udf_type_string(return_type);
        build_udf_cloud_script(code, handler, imports, packages, &input_types, &result_type)
    }

    async fn resolve_udf_with_stage(&mut self, code: String) -> Result<Vec<u8>> {
        let file_location = match code.strip_prefix('@') {
            Some(location) => FileLocation::Stage(location.to_string()),
            None => {
                let uri = UriLocation::from_uri(code.clone(), BTreeMap::default());

                match uri {
                    Ok(uri) => FileLocation::Uri(uri),
                    Err(_) => {
                        // fallback to use the code as real code
                        return Ok(code.into());
                    }
                }
            }
        };

        let (stage_info, module_path) = resolve_file_location(self.ctx.as_ref(), &file_location)
            .await
            .map_err(|err| {
                ErrorCode::SemanticError(format!(
                    "Failed to resolve code location {:?}: {}",
                    code, err
                ))
            })?;

        let op = init_stage_operator(&stage_info).map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to get StageTable operator: {}", err))
        })?;

        let code_blob = op
            .read(&module_path)
            .await
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

    fn resolve_udf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[Expr],
        udf_definition: UDFScript,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let UDFScript {
            code,
            handler,
            language,
            arg_types,
            return_type,
            runtime_version,
            imports,
            packages,
            immutable,
        } = udf_definition;

        let language = language.parse()?;
        let use_cloud = matches!(language, UDFLanguage::Python)
            && GlobalConfig::instance().query.common.enable_udf_sandbox;
        if use_cloud {
            UDFValidator::is_udf_cloud_script_allowed(&language)?;
        } else {
            UDFValidator::is_udf_script_allowed(&language)?;
        }
        let mut arguments = Vec::with_capacity(args.len());
        for (argument, dest_type) in args.iter().zip(arg_types.iter()) {
            let box (arg, ty) = self.resolve(argument)?;
            if ty != *dest_type {
                arguments.push(wrap_cast(&arg, dest_type));
            } else {
                arguments.push(arg);
            }
        }

        if use_cloud {
            let code_bytes =
                databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?;
            let resolved_code = String::from_utf8(code_bytes).map_err(|err| {
                ErrorCode::SemanticError(format!("Failed to parse UDF code as utf-8: {err}"))
            })?;
            let settings = self.ctx.get_settings();
            let import_assets = self.build_udf_cloud_imports(
                &imports,
                Duration::from_secs(settings.get_udf_cloud_import_presign_expire_secs()?),
            )?;
            let mut merged_packages = extract_script_metadata_deps(&resolved_code);
            merged_packages.extend_from_slice(&packages);
            let script = self.build_udf_cloud_script(
                &resolved_code,
                &handler,
                &import_assets,
                &merged_packages,
                &arg_types,
                &return_type,
            )?;
            let (endpoint, headers) = self.apply_udf_cloud_resource(&name, "udf", script)?;
            let udf_definition = UDFServer {
                address: endpoint,
                handler,
                headers,
                language: language.to_string(),
                arg_names: Vec::new(),
                arg_types,
                return_type,
                immutable,
            };
            return self.resolve_udf_server_internal(span, name, args, udf_definition, false);
        }

        let code_blob = databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?
            .into_boxed_slice();

        let imports_stage_info = databend_common_base::runtime::block_on(resolve_stage_locations(
            self.ctx.as_ref(),
            &imports
                .iter()
                .map(|s| s.trim_start_matches('@').to_string())
                .collect::<Vec<String>>(),
        ))?;

        let udf_type = UDFType::Script(Box::new(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
            imports_stage_info,
            imports,
            packages,
        }));

        let arg_names = args.iter().map(|arg| format!("{arg}")).join(", ");
        let display_name = format!("{}({})", &handler, arg_names);

        self.bind_context.have_udf_script = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDFCall {
                span,
                name,
                handler,
                headers: BTreeMap::default(),
                display_name,
                arg_types,
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments,
            }
            .into(),
            return_type,
        )))
    }

    fn resolve_udaf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[Expr],
        udf_definition: UDAFScript,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
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
        let code_blob = databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?
            .into_boxed_slice();
        let imports_stage_info = databend_common_base::runtime::block_on(resolve_stage_locations(
            self.ctx.as_ref(),
            &imports
                .iter()
                .map(|s| s.trim_start_matches('@').to_string())
                .collect::<Vec<String>>(),
        ))?;

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
                let box (arg, ty) = self.resolve(argument)?;
                Ok(if ty == *dest_type {
                    arg
                } else {
                    wrap_cast(&arg, dest_type)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let display_name = format!(
            "{name}({})",
            args.iter().map(|arg| format!("{:#}", arg)).join(", ")
        );

        self.bind_context.have_udf_script = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
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
        )))
    }

    fn resolve_lambda_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[Expr],
        udf_definition: LambdaUDF,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let parameters = udf_definition.parameters;
        if parameters.len() != arguments.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Require {} parameters, but got: {}",
                parameters.len(),
                arguments.len()
            ))
            .set_span(span));
        }
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let sql_tokens = tokenize_sql(udf_definition.definition.as_str())?;
        let expr = parse_expr(&sql_tokens, sql_dialect)?;
        let mut args_map = HashMap::new();
        arguments.iter().enumerate().for_each(|(idx, argument)| {
            if let Some(parameter) = parameters.get(idx) {
                args_map.insert(parameter.as_str(), (*argument).clone());
            }
        });

        let udf_expr = Self::clone_expr_with_replacement(&expr, |nest_expr| {
            if let Expr::ColumnRef { column, .. } = nest_expr {
                if let Some(arg) = args_map.get(column.column.name()) {
                    return Ok(Some(arg.clone()));
                }
            }
            Ok(None)
        })
        .map_err(|e| e.set_span(span))?;
        let scalar = self.resolve(&udf_expr)?;
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(scalar.0),
            }
            .into(),
            scalar.1,
        )))
    }

    fn resolve_scalar_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[Expr],
        udf_definition: ScalarUDF,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let arg_types = udf_definition.arg_types;
        if arg_types.len() != arguments.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Require {} parameters, but got: {}",
                arg_types.len(),
                arguments.len()
            ))
            .set_span(span));
        }
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let sql_tokens = tokenize_sql(udf_definition.definition.as_str())?;
        let mut udf_expr = parse_expr(&sql_tokens, sql_dialect)?;
        let mut visitor = UDFArgVisitor::new(&arg_types, arguments);
        udf_expr.drive_mut(&mut visitor);

        // Use current binding context so column references inside arguments can be resolved.
        let box (expr, _) = TypeChecker::try_create(
            self.bind_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
            self.forbid_udf,
        )?
        .resolve(&udf_expr)?;
        let return_ty = udf_definition.return_type;
        let expr = CastExpr {
            span,
            is_try: false,
            argument: Box::new(expr),
            target_type: Box::new(return_ty.clone()),
        };
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(expr.into()),
            }
            .into(),
            return_ty,
        )))
    }
}
