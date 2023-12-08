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

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use common_ast::ast::AlterUDFStmt;
use common_ast::ast::CreateUDFStmt;
use common_ast::ast::Identifier;
use common_ast::ast::UDFDefinition;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::udf_client::UDFFlightClient;
use common_meta_app::principal::LambdaUDF;
use common_meta_app::principal::UDFDefinition as PlanUDFDefinition;
use common_meta_app::principal::UDFServer;
use common_meta_app::principal::UserDefinedFunction;

use crate::optimizer::SExpr;
use crate::planner::resolve_type_name;
use crate::planner::udf_validator::UDFValidator;
use crate::plans::walk_expr_mut;
use crate::plans::AlterUDFPlan;
use crate::plans::BoundColumnRef;
use crate::plans::CreateUDFPlan;
use crate::plans::EvalScalar;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UDFServerCall;
use crate::plans::Udf;
use crate::plans::VisitorMut;
use crate::Binder;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct UdfInfo {
    /// Arguments of udf functions
    pub udf_arguments: Vec<ScalarItem>,
    /// Udf functions
    pub udf_functions: Vec<ScalarItem>,
    /// Mapping: (udf function display name) -> (derived column ref)
    /// This is used to replace udf with a derived column.
    pub udf_functions_map: HashMap<String, BoundColumnRef>,
    /// Mapping: (udf function display name) -> (derived index)
    /// This is used to reuse already generated derived columns
    pub udf_functions_index_map: HashMap<String, IndexType>,
}

pub(crate) struct UdfRewriter {
    udf_info: UdfInfo,
    metadata: MetadataRef,
}

impl UdfRewriter {
    pub(crate) fn new(metadata: MetadataRef) -> Self {
        Self {
            udf_info: Default::default(),
            metadata,
        }
    }

    pub(crate) fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        if !s_expr.children.is_empty() {
            let mut children = Vec::with_capacity(s_expr.children.len());
            for child in s_expr.children.iter() {
                children.push(Arc::new(self.rewrite(child)?));
            }
            s_expr.children = children;
        }

        // Rewrite Udf and its arguments as derived column.
        match (*s_expr.plan).clone() {
            RelOperator::EvalScalar(mut plan) => {
                for item in &plan.items {
                    // The index of Udf item can be reused.
                    if let ScalarExpr::UDFServerCall(udf) = &item.scalar {
                        self.udf_info
                            .udf_functions_index_map
                            .insert(udf.display_name.clone(), item.index);
                    }
                }
                for item in &mut plan.items {
                    self.visit(&mut item.scalar)?;
                }
                let child_expr = self.create_udf_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            RelOperator::Filter(mut plan) => {
                for scalar in &mut plan.predicates {
                    self.visit(scalar)?;
                }
                let child_expr = self.create_udf_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            _ => Ok(s_expr),
        }
    }

    fn create_udf_expr(&mut self, mut child_expr: Arc<SExpr>) -> Arc<SExpr> {
        let udf_info = &mut self.udf_info;
        if !udf_info.udf_functions.is_empty() {
            if !udf_info.udf_arguments.is_empty() {
                // Add an EvalScalar for the arguments of Udf.
                let mut scalar_items = mem::take(&mut udf_info.udf_arguments);
                scalar_items.sort_by_key(|item| item.index);
                let eval_scalar = EvalScalar {
                    items: scalar_items,
                };
                child_expr = Arc::new(SExpr::create_unary(
                    Arc::new(eval_scalar.into()),
                    child_expr,
                ));
            }

            let udf_plan = Udf {
                items: mem::take(&mut udf_info.udf_functions),
            };
            Arc::new(SExpr::create_unary(Arc::new(udf_plan.into()), child_expr))
        } else {
            child_expr
        }
    }
}

impl<'a> VisitorMut<'a> for UdfRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        walk_expr_mut(self, expr)?;
        // replace udf with derived column
        if let ScalarExpr::UDFServerCall(udf) = expr {
            if let Some(column_ref) = self.udf_info.udf_functions_map.get(&udf.display_name) {
                *expr = ScalarExpr::BoundColumnRef(column_ref.clone());
            } else {
                return Err(ErrorCode::Internal("Rewrite udf function failed"));
            }
        }
        Ok(())
    }

    fn visit_udf_server_call(&mut self, udf: &'a mut UDFServerCall) -> Result<()> {
        for (i, arg) in udf.arguments.iter_mut().enumerate() {
            if let ScalarExpr::UDFServerCall(_) = arg {
                return Err(ErrorCode::InvalidArgument(
                    "the argument of UDF server call can't be a UDF server call",
                ));
            }
            self.visit(arg)?;

            let new_column_ref = if let ScalarExpr::BoundColumnRef(ref column_ref) = &arg {
                column_ref.clone()
            } else {
                let name = format!("{}_arg_{}", &udf.display_name, i);
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(name.clone(), arg.data_type()?);

                // Generate a ColumnBinding for each argument of udf function
                let column = ColumnBindingBuilder::new(
                    name,
                    index,
                    Box::new(arg.data_type()?),
                    Visibility::Visible,
                )
                .build();

                BoundColumnRef {
                    span: arg.span(),
                    column,
                }
            };

            self.udf_info.udf_arguments.push(ScalarItem {
                index: new_column_ref.column.index,
                scalar: arg.clone(),
            });
            *arg = new_column_ref.into();
        }

        let index = match self.udf_info.udf_functions_index_map.get(&udf.display_name) {
            Some(index) => *index,
            None => self
                .metadata
                .write()
                .add_derived_column(udf.display_name.clone(), (*udf.return_type).clone()),
        };

        // Generate a ColumnBinding for the udf function
        let column = ColumnBindingBuilder::new(
            udf.display_name.clone(),
            index,
            udf.return_type.clone(),
            Visibility::Visible,
        )
        .build();

        let replaced_column = BoundColumnRef {
            span: udf.span,
            column,
        };

        self.udf_info
            .udf_functions_map
            .insert(udf.display_name.clone(), replaced_column);
        self.udf_info.udf_functions.push(ScalarItem {
            index,
            scalar: udf.clone().into(),
        });

        Ok(())
    }
}

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
                if !GlobalConfig::instance().query.enable_udf_server {
                    return Err(ErrorCode::Unimplemented(
                        "UDF server is not allowed, you can enable it by setting 'enable_udf_server = true' in query node config",
                    ));
                }

                let udf_server_allow_list = &GlobalConfig::instance().query.udf_server_allow_list;
                if udf_server_allow_list
                    .iter()
                    .all(|addr| addr.trim_end_matches('/') != address.trim_end_matches('/'))
                {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "Unallowed UDF server address, '{address}' is not in udf_server_allow_list"
                    )));
                }

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
                )
                .await?;
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
