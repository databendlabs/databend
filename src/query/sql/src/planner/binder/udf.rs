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

use super::select::SelectList;
use crate::optimizer::SExpr;
use crate::planner::resolve_type_name;
use crate::planner::udf_validator::UDFValidator;
use crate::plans::AggregateFunction;
use crate::plans::AlterUDFPlan;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::CreateUDFPlan;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::LambdaFunc;
use crate::plans::Plan;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UDFServerCall;
use crate::plans::Udf;
use crate::plans::WindowFunc;
use crate::plans::WindowOrderBy;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBindingBuilder;
use crate::MetadataRef;
use crate::Visibility;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct UdfInfo {
    /// Arguments of udf functions
    pub udf_arguments: Vec<ScalarItem>,
    /// Udf functions
    pub udf_functions: Vec<ScalarItem>,
    /// Mapping: (udf function display name) -> (derived column ref)
    /// This is used to generate column in projection.
    pub udf_functions_map: HashMap<String, BoundColumnRef>,
}

pub(super) struct UdfRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
}

impl<'a> UdfRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }

    pub fn visit(&mut self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => Ok(scalar.clone()),
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::FunctionCall(func) => {
                let new_args = func
                    .arguments
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FunctionCall {
                    span: func.span,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments: new_args,
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.visit(&cast.argument)?),
                target_type: cast.target_type.clone(),
            }
            .into()),

            ScalarExpr::UDFServerCall(udf) => {
                let mut replaced_args = Vec::with_capacity(udf.arguments.len());
                for (i, arg) in udf.arguments.iter().enumerate() {
                    let new_arg = self.visit(arg)?;
                    if let ScalarExpr::UDFServerCall(_) = new_arg {
                        replaced_args.push(new_arg);
                        continue;
                    }

                    let replaced_arg = if let ScalarExpr::BoundColumnRef(ref column_ref) = new_arg {
                        column_ref.clone()
                    } else {
                        let name = format!("{}_arg_{}", &udf.display_name, i);
                        let index = self
                            .metadata
                            .write()
                            .add_derived_column(name.clone(), new_arg.data_type()?);

                        // Generate a ColumnBinding for each argument of udf function
                        let column = ColumnBindingBuilder::new(
                            name,
                            index,
                            Box::new(new_arg.data_type()?),
                            Visibility::Visible,
                        )
                        .build();

                        BoundColumnRef {
                            span: new_arg.span(),
                            column,
                        }
                    };

                    self.bind_context.udf_info.udf_arguments.push(ScalarItem {
                        index: replaced_arg.column.index,
                        scalar: new_arg,
                    });
                    replaced_args.push(replaced_arg.into());
                }

                let index = self
                    .metadata
                    .write()
                    .add_derived_column(udf.display_name.clone(), scalar.data_type()?);

                let column = ColumnBindingBuilder::new(
                    udf.display_name.clone(),
                    index,
                    Box::new(scalar.data_type()?),
                    Visibility::Visible,
                )
                .build();

                let replaced_column = BoundColumnRef {
                    span: scalar.span(),
                    column,
                };

                let replaced_udf = UDFServerCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    display_name: udf.display_name.clone(),
                    server_addr: udf.server_addr.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments: replaced_args,
                };

                self.bind_context
                    .udf_info
                    .udf_functions_map
                    .insert(udf.display_name.clone(), replaced_column);
                self.bind_context.udf_info.udf_functions.push(ScalarItem {
                    index,
                    scalar: replaced_udf.clone().into(),
                });

                Ok(replaced_udf.into())
            }

            // TODO(leiysky): should we recursively process subquery here?
            ScalarExpr::SubqueryExpr(_) => Ok(scalar.clone()),

            ScalarExpr::AggregateFunction(agg_func) => {
                let new_args = agg_func
                    .args
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(AggregateFunction {
                    func_name: agg_func.func_name.clone(),
                    distinct: agg_func.distinct,
                    params: agg_func.params.clone(),
                    args: new_args,
                    return_type: agg_func.return_type.clone(),
                    display_name: agg_func.display_name.clone(),
                }
                .into())
            }

            ScalarExpr::WindowFunction(window) => {
                let new_partition_by = window
                    .partition_by
                    .iter()
                    .map(|partition_by| self.visit(partition_by))
                    .collect::<Result<Vec<_>>>()?;

                let mut new_order_by = Vec::with_capacity(window.order_by.len());
                for order_by in window.order_by.iter() {
                    new_order_by.push(WindowOrderBy {
                        expr: self.visit(&order_by.expr)?,
                        asc: order_by.asc,
                        nulls_first: order_by.nulls_first,
                    });
                }

                Ok(WindowFunc {
                    span: window.span,
                    display_name: window.display_name.clone(),
                    partition_by: new_partition_by,
                    func: window.func.clone(),
                    order_by: new_order_by,
                    frame: window.frame.clone(),
                }
                .into())
            }

            ScalarExpr::LambdaFunction(lambda_func) => {
                let new_args = lambda_func
                    .args
                    .iter()
                    .map(|arg| self.visit(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LambdaFunc {
                    span: lambda_func.span,
                    func_name: lambda_func.func_name.clone(),
                    display_name: lambda_func.display_name.clone(),
                    args: new_args,
                    params: lambda_func.params.clone(),
                    lambda_expr: lambda_func.lambda_expr.clone(),
                    return_type: lambda_func.return_type.clone(),
                }
                .into())
            }
        }
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

    /// Analyze udf functions in select clause, this will rewrite udf functions.
    /// See [`UdfRewriter`] for more details.
    pub(crate) fn analyze_udf(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = UdfRewriter::new(bind_context, self.metadata.clone());
            let new_scalar = rewriter.visit(&item.scalar)?;
            item.scalar = new_scalar;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn bind_udf(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        let udf_info = &bind_context.udf_info;
        if udf_info.udf_functions.is_empty() {
            return Ok(child);
        }

        let mut new_expr = child;
        if !udf_info.udf_arguments.is_empty() {
            let mut scalar_items = udf_info.udf_arguments.clone();
            scalar_items.sort_by_key(|item| item.index);
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(new_expr));
        }

        let udf_plan = Udf {
            items: udf_info.udf_functions.clone(),
        };
        new_expr = SExpr::create_unary(Arc::new(udf_plan.into()), Arc::new(new_expr));

        Ok(new_expr)
    }
}
