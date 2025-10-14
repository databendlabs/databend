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

use std::collections::HashSet;

use chrono::Utc;
use databend_common_ast::ast::AlterUDFStmt;
use databend_common_ast::ast::CreateUDFStmt;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UDAFStateField;
use databend_common_ast::ast::UDFArgs;
use databend_common_ast::ast::UDFDefinition;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::DataField;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::ScalarUDF;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition as PlanUDFDefinition;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::principal::UDTF;

use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::planner::expression::UDFValidator;
use crate::planner::resolve_type_name_udf;
use crate::plans::AlterUDFPlan;
use crate::plans::CreateUDFPlan;
use crate::plans::DropUDFPlan;
use crate::plans::Plan;
use crate::plans::UDFLanguage;
use crate::BindContext;
use crate::Binder;
use crate::UdfRewriter;

impl Binder {
    pub(in crate::planner::binder) async fn bind_udf_definition(
        &mut self,
        udf_name: &Identifier,
        udf_description: &Option<String>,
        udf_definition: &UDFDefinition,
    ) -> Result<UserDefinedFunction> {
        let name = normalize_identifier(udf_name, &self.name_resolution_ctx).to_string();
        let description = udf_description.clone().unwrap_or_default();
        match udf_definition {
            UDFDefinition::LambdaUDF {
                parameters,
                definition,
            } => {
                let mut validator = UDFValidator {
                    name,
                    parameters: parameters.iter().map(|v| v.to_string()).collect(),
                    ..Default::default()
                };
                validator.verify_definition_expr(definition)?;
                Ok(UserDefinedFunction {
                    name: validator.name,
                    description,
                    definition: PlanUDFDefinition::LambdaUDF(LambdaUDF {
                        parameters: validator.parameters,
                        definition: definition.to_string(),
                    }),
                    created_on: Utc::now(),
                })
            }
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                address,
                handler,
                headers,
                language,
                immutable,
            } => {
                UDFValidator::is_udf_server_allowed(address.as_str())?;

                let mut arg_datatypes = Vec::with_capacity(arg_types.len());
                let mut arg_names = Vec::with_capacity(arg_types.len());
                match arg_types {
                    UDFArgs::Types(types) => {
                        for arg_type in types {
                            if matches!(arg_type, TypeName::StageLocation) {
                                return Err(ErrorCode::InvalidArgument(
                                    "StageLocation must have a corresponding variable name",
                                ));
                            }
                            arg_datatypes.push(DataType::from(&resolve_type_name_udf(arg_type)?));
                        }
                    }
                    UDFArgs::NameWithTypes(name_with_types) => {
                        for (arg_name, arg_type) in name_with_types {
                            arg_names.push(
                                normalize_identifier(arg_name, &self.name_resolution_ctx).name,
                            );
                            arg_datatypes.push(DataType::from(&resolve_type_name_udf(arg_type)?));
                        }
                    }
                }
                let return_type = DataType::from(&resolve_type_name_udf(return_type)?);

                let connect_timeout = self
                    .ctx
                    .get_settings()
                    .get_external_server_connect_timeout_secs()?;
                let request_timeout = self
                    .ctx
                    .get_settings()
                    .get_external_server_request_timeout_secs()?;
                let batch_rows =
                    self.ctx
                        .get_settings()
                        .get_external_server_request_batch_rows()? as usize;

                let endpoint = UDFFlightClient::build_endpoint(
                    address,
                    connect_timeout,
                    request_timeout,
                    &self.ctx.get_version().udf_client_user_agent(),
                )?;

                let mut client =
                    UDFFlightClient::connect(handler, endpoint, connect_timeout, batch_rows)
                        .await?
                        .with_tenant(self.ctx.get_tenant().tenant_name())?
                        .with_func_name(&name)?
                        .with_handler_name(handler)?
                        .with_query_id(&self.ctx.get_id())?
                        .with_headers(headers.iter())?;
                client
                    .check_schema(handler, &arg_datatypes, &return_type)
                    .await?;

                Ok(UserDefinedFunction {
                    name,
                    description,
                    definition: PlanUDFDefinition::UDFServer(UDFServer {
                        address: address.clone(),
                        arg_names,
                        arg_types: arg_datatypes,
                        return_type,
                        handler: handler.clone(),
                        headers: headers.clone(),
                        language: language.clone(),
                        immutable: *immutable,
                    }),
                    created_on: Utc::now(),
                })
            }
            UDFDefinition::UDAFServer { .. } => unimplemented!(),
            UDFDefinition::UDFScript {
                arg_types,
                return_type,
                code,
                handler,
                language,
                runtime_version,
                imports,
                packages,
                immutable,
            } => {
                UDFValidator::is_udf_script_allowed(&language.parse()?)?;
                let definition = create_udf_definition_script(
                    arg_types,
                    None,
                    return_type,
                    runtime_version,
                    imports,
                    packages,
                    handler,
                    language,
                    code,
                    *immutable,
                )?;
                Ok(UserDefinedFunction {
                    name,
                    description,
                    definition,
                    created_on: Utc::now(),
                })
            }
            UDFDefinition::UDAFScript {
                arg_types,
                state_fields,
                return_type,
                code,
                language,
                runtime_version,
                imports,
                packages,
            } => {
                let definition = create_udf_definition_script(
                    arg_types,
                    Some(state_fields),
                    return_type,
                    runtime_version,
                    imports,
                    packages,
                    "",
                    language,
                    code,
                    None,
                )?;
                Ok(UserDefinedFunction {
                    name,
                    description,
                    definition,
                    created_on: Utc::now(),
                })
            }
            UDFDefinition::UDTFSql {
                arg_types,
                return_types,
                sql,
            } => {
                let arg_types = arg_types
                    .iter()
                    .map(|(name, arg_type)| {
                        let column = normalize_identifier(name, &self.name_resolution_ctx).name;
                        let ty = DataType::from(&resolve_type_name_udf(arg_type)?);
                        Ok((column, ty))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let return_types = return_types
                    .iter()
                    .map(|(name, arg_type)| {
                        let column = normalize_identifier(name, &self.name_resolution_ctx).name;
                        let ty = DataType::from(&resolve_type_name_udf(arg_type)?);
                        Ok((column, ty))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(UserDefinedFunction {
                    name,
                    description,
                    definition: PlanUDFDefinition::UDTF(UDTF {
                        arg_types,
                        return_types,
                        sql: sql.to_string(),
                    }),
                    created_on: Utc::now(),
                })
            }
            UDFDefinition::ScalarUDF {
                arg_types,
                definition,
                return_type,
            } => {
                let arg_types = arg_types
                    .iter()
                    .map(|(name, arg_type)| {
                        let column = normalize_identifier(name, &self.name_resolution_ctx).name;
                        let ty = DataType::from(&resolve_type_name_udf(arg_type)?);
                        Ok((column, ty))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let return_type = DataType::from(&resolve_type_name_udf(return_type)?);

                Ok(UserDefinedFunction {
                    name,
                    description,
                    definition: PlanUDFDefinition::ScalarUDF(ScalarUDF {
                        arg_types,
                        return_type,
                        definition: definition.clone(),
                    }),
                    created_on: Utc::now(),
                })
            }
        }
    }

    pub(in crate::planner::binder) async fn bind_create_udf(
        &mut self,
        stmt: &CreateUDFStmt,
    ) -> Result<Plan> {
        let udf = self
            .bind_udf_definition(&stmt.udf_name, &stmt.description, &stmt.definition)
            .await?;
        Ok(Plan::CreateUDF(Box::new(CreateUDFPlan {
            create_option: stmt.create_option.clone().into(),
            udf,
        })))
    }

    pub(in crate::planner::binder) async fn bind_alter_udf(
        &mut self,
        stmt: &AlterUDFStmt,
    ) -> Result<Plan> {
        let udf = self
            .bind_udf_definition(&stmt.udf_name, &stmt.description, &stmt.definition)
            .await?;
        Ok(Plan::AlterUDF(Box::new(AlterUDFPlan { udf })))
    }

    pub(in crate::planner::binder) async fn bind_drop_udf(
        &mut self,
        if_exists: bool,
        udf_name: &Identifier,
    ) -> Result<Plan> {
        let name = normalize_identifier(udf_name, &self.name_resolution_ctx).to_string();
        Ok(Plan::DropUDF(Box::new(DropUDFPlan {
            if_exists,
            udf: name,
        })))
    }

    // Rewrite udf script, udf server and its arguments as derived column.
    pub(in crate::planner::binder) fn rewrite_udf(
        &mut self,
        bind_context: &mut BindContext,
        s_expr: SExpr,
    ) -> Result<SExpr> {
        if !bind_context.have_udf_script && !bind_context.have_udf_server {
            return Ok(s_expr);
        }
        let mut s_expr = s_expr.clone();
        if bind_context.have_udf_script {
            // rewrite udf for interpreter udf
            let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), true);
            s_expr = udf_rewriter.rewrite(&s_expr)?;
        }
        if bind_context.have_udf_server {
            // rewrite udf for server udf
            let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), false);
            s_expr = udf_rewriter.rewrite(&s_expr)?;
        }
        Ok(s_expr)
    }
}

fn create_udf_definition_script(
    arg_types: &[TypeName],
    state_fields: Option<&[UDAFStateField]>,
    return_type: &TypeName,
    runtime_version: &str,
    imports: &[String],
    packages: &[String],
    handler: &str,
    language: &str,
    code: &str,
    immutable: Option<bool>,
) -> Result<PlanUDFDefinition> {
    let Ok(language) = language.parse::<UDFLanguage>() else {
        return Err(ErrorCode::InvalidArgument(format!(
            "Unallowed UDF language {language:?}, must be python, javascript or wasm"
        )));
    };

    let arg_types = arg_types
        .iter()
        .map(|arg_type| Ok(DataType::from(&resolve_type_name_udf(arg_type)?)))
        .collect::<Result<Vec<_>>>()?;

    let return_type = DataType::from(&resolve_type_name_udf(return_type)?);

    let mut runtime_version = runtime_version.to_string();
    if runtime_version.is_empty() && language == UDFLanguage::Python {
        runtime_version = "3.12.2".to_string();
    }

    match state_fields {
        Some(fields) => {
            let state_fields = fields
                .iter()
                .map(|field| {
                    Ok(DataField::new(
                        &field.name.name,
                        DataType::from(&resolve_type_name_udf(&field.type_name)?),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            let state_field_names = state_fields
                .iter()
                .map(|f| f.name())
                .collect::<HashSet<_>>();
            if state_field_names.len() != state_fields.len() {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Duplicate state field name in UDAF script"
                )));
            }

            Ok(PlanUDFDefinition::UDAFScript(UDAFScript {
                code: code.to_string(),
                arg_types,
                imports: imports.to_vec(),
                packages: packages.to_vec(),
                state_fields,
                return_type,
                language: language.to_string(),
                runtime_version,
            }))
        }
        None => Ok(PlanUDFDefinition::UDFScript(UDFScript {
            code: code.to_string(),
            arg_types,
            return_type,
            imports: imports.to_vec(),
            packages: packages.to_vec(),
            handler: handler.to_string(),
            language: language.to_string(),
            runtime_version,
            immutable,
        })),
    }
}
