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

use common_ast::ast::AlterUDFStmt;
use common_ast::ast::CreateUDFStmt;
use common_ast::ast::Identifier;
use common_ast::ast::UDFDefinition;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::udf_client::UDFFlightClient;
use common_meta_app::principal::LambdaUDF;
use common_meta_app::principal::UDFDefinition as PlanUDFDefinition;
use common_meta_app::principal::UDFServer;
use common_meta_app::principal::UserDefinedFunction;

use crate::planner::resolve_type_name;
use crate::planner::udf_validator::UDFValidator;
use crate::plans::AlterUDFPlan;
use crate::plans::CreateUDFPlan;
use crate::plans::Plan;
use crate::Binder;

impl Binder {
    pub(in crate::planner::binder) async fn bind_udf_definition(
        &mut self,
        udf_name: &Identifier,
        udf_description: &Option<String>,
        udf_definition: &UDFDefinition,
    ) -> Result<UserDefinedFunction> {
        match udf_definition {
            UDFDefinition::LambdaUDF {
                parameters,
                definition,
            } => {
                let mut validator = UDFValidator {
                    name: udf_name.to_string(),
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
                })
            }
            UDFDefinition::UDFServer {
                arg_types,
                return_type,
                address,
                handler,
                language,
            } => {
                let mut arg_datatypes = Vec::with_capacity(arg_types.len());
                for arg_type in arg_types {
                    arg_datatypes.push(DataType::from(&resolve_type_name(arg_type)?));
                }
                let return_type = DataType::from(&resolve_type_name(return_type)?);

                let mut client = UDFFlightClient::connect(address).await?;
                client
                    .check_schema(handler, &arg_datatypes, &return_type)
                    .await?;

                Ok(UserDefinedFunction {
                    name: udf_name.to_string(),
                    description: udf_description.clone().unwrap_or_default(),
                    definition: PlanUDFDefinition::UDFServer(UDFServer {
                        address: address.clone(),
                        arg_types: arg_datatypes,
                        return_type,
                        handler: handler.clone(),
                        language: language.clone(),
                    }),
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
            if_not_exists: stmt.if_not_exists,
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
}
