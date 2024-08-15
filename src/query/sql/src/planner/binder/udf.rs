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
use databend_common_ast::ast::UDFDefinition;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::UDFDefinition as PlanUDFDefinition;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::principal::UserDefinedFunction;

use crate::normalize_identifier;
use crate::planner::resolve_type_name;
use crate::planner::udf_validator::UDFValidator;
use crate::plans::AlterUDFPlan;
use crate::plans::CreateUDFPlan;
use crate::plans::DropUDFPlan;
use crate::plans::Plan;
use crate::Binder;

impl Binder {
    fn is_allowed_language(language: &str) -> bool {
        let allowed_languages: HashSet<&str> =
            ["javascript", "wasm", "python"].iter().cloned().collect();
        allowed_languages.contains(&language.to_lowercase().as_str())
    }

    pub(in crate::planner::binder) async fn bind_udf_definition(
        &mut self,
        udf_name: &Identifier,
        udf_description: &Option<String>,
        udf_definition: &UDFDefinition,
    ) -> Result<UserDefinedFunction> {
        let name = normalize_identifier(udf_name, &self.name_resolution_ctx).to_string();
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
                    description: udf_description.clone().unwrap_or_default(),
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
                language,
            } => {
                UDFValidator::is_udf_server_allowed(address.as_str())?;

                let mut arg_datatypes = Vec::with_capacity(arg_types.len());
                for arg_type in arg_types {
                    arg_datatypes.push(DataType::from(&resolve_type_name(arg_type, true)?));
                }
                let return_type = DataType::from(&resolve_type_name(return_type, true)?);

                let mut client = UDFFlightClient::connect(
                    address,
                    self.ctx
                        .get_settings()
                        .get_external_server_connect_timeout_secs()?,
                    self.ctx
                        .get_settings()
                        .get_external_server_request_timeout_secs()?,
                    self.ctx
                        .get_settings()
                        .get_external_server_request_batch_rows()?,
                )
                .await?;
                client
                    .check_schema(handler, &arg_datatypes, &return_type)
                    .await?;

                Ok(UserDefinedFunction {
                    name,
                    description: udf_description.clone().unwrap_or_default(),
                    definition: PlanUDFDefinition::UDFServer(UDFServer {
                        address: address.clone(),
                        arg_types: arg_datatypes,
                        return_type,
                        handler: handler.clone(),
                        language: language.clone(),
                    }),
                    created_on: Utc::now(),
                })
            }
            UDFDefinition::UDFScript {
                arg_types,
                return_type,
                code,
                handler,
                language,
                runtime_version,
            } => {
                let mut arg_datatypes = Vec::with_capacity(arg_types.len());
                for arg_type in arg_types {
                    arg_datatypes.push(DataType::from(&resolve_type_name(arg_type, true)?));
                }
                let return_type = DataType::from(&resolve_type_name(return_type, true)?);

                if !Self::is_allowed_language(language) {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "Unallowed UDF language '{language}', must be python, javascript or wasm"
                    )));
                }

                let mut runtime_version = runtime_version.to_string();
                if runtime_version.is_empty() && language.to_lowercase() == "python" {
                    runtime_version = "3.12.2".to_string();
                }

                Ok(UserDefinedFunction {
                    name,
                    description: udf_description.clone().unwrap_or_default(),
                    definition: PlanUDFDefinition::UDFScript(UDFScript {
                        code: code.clone(),
                        arg_types: arg_datatypes,
                        return_type,
                        handler: handler.clone(),
                        language: language.clone(),
                        runtime_version,
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
}
