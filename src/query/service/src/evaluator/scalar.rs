// Copyright 2022 Datafuse Labs.
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

use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::in_evalutor;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;

use crate::evaluator::eval_node::EvalNode;
use crate::evaluator::Evaluator;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;

impl Evaluator {
    pub fn eval_scalar(scalar: &Scalar) -> Result<EvalNode> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => Ok(EvalNode::Variable {
                name: column_ref.column.index.to_string(),
            }),
            Scalar::ConstantExpr(constant) => Ok(EvalNode::Constant {
                value: constant.value.clone(),
                data_type: *constant.data_type.clone(),
            }),
            Scalar::AndExpr(and) => {
                let args = vec![
                    Self::eval_scalar(&and.left)?,
                    Self::eval_scalar(&and.right)?,
                ];
                let func = FunctionFactory::instance()
                    .get("and", &[&and.left.data_type(), &and.right.data_type()])?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::OrExpr(or) => {
                let args = vec![Self::eval_scalar(&or.left)?, Self::eval_scalar(&or.right)?];
                let func = FunctionFactory::instance()
                    .get("or", &[&or.left.data_type(), &or.right.data_type()])?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::ComparisonExpr(comp) => {
                let args = vec![
                    Self::eval_scalar(&comp.left)?,
                    Self::eval_scalar(&comp.right)?,
                ];
                let func = FunctionFactory::instance().get(comp.op.to_func_name(), &[
                    &comp.left.data_type(),
                    &comp.right.data_type(),
                ])?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::FunctionCall(func) => {
                let eval_args: Vec<EvalNode> = func
                    .arguments
                    .iter()
                    .map(Self::eval_scalar)
                    .collect::<Result<_>>()?;

                // special case for in function
                let name_lower = func.func_name.to_lowercase();
                if name_lower.as_str() == "in" || name_lower.as_str() == "not_in" {
                    if let EvalNode::Constant {
                        value: DataValue::Struct(vs),
                        ..
                    } = &eval_args[1]
                    {
                        let func = if name_lower.as_str() == "not_in" {
                            in_evalutor::create_by_values::<true>(
                                func.arg_types[1].clone(),
                                vs.clone(),
                            )
                        } else {
                            in_evalutor::create_by_values::<false>(
                                func.arg_types[1].clone(),
                                vs.clone(),
                            )
                        }?;

                        return Ok(EvalNode::Function {
                            func,
                            args: vec![eval_args[0].clone()],
                        });
                    } else {
                        return Err(ErrorCode::SyntaxException(
                            "IN expression must have a literal array or subquery as the second argument",
                        ));
                    }
                }

                let arg_types: Vec<&DataTypeImpl> = func.arg_types.iter().collect();
                let func = FunctionFactory::instance().get(func.func_name.as_str(), &arg_types)?;
                Ok(EvalNode::Function {
                    func,
                    args: eval_args,
                })
            }
            Scalar::CastExpr(cast) => {
                let arg = Self::eval_scalar(&cast.argument)?;
                let func = if cast.target_type.is_nullable() {
                    CastFunction::create_try(
                        "",
                        cast.target_type.name().as_str(),
                        *cast.from_type.clone(),
                    )?
                } else {
                    CastFunction::create(
                        "",
                        cast.target_type.name().as_str(),
                        *cast.from_type.clone(),
                    )?
                };
                Ok(EvalNode::Function {
                    func,
                    args: vec![arg],
                })
            }

            Scalar::SubqueryExpr(_) => Err(ErrorCode::LogicalError(
                "Cannot evaluate subquery expression",
            )),
            Scalar::AggregateFunction(_) => Err(ErrorCode::LogicalError(
                "Cannot evaluate aggregate function",
            )),
        }
    }
}
