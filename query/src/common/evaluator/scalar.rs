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
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;

use crate::common::evaluator::eval_node::EvalNode;
use crate::common::evaluator::Evaluator;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;

impl Evaluator {
    pub fn eval_scalar<VectorID>(scalar: &Scalar) -> Result<EvalNode<VectorID>>
    where VectorID: From<String> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => Ok(EvalNode::Variable {
                id: column_ref.column.index.to_string().into(),
            }),
            Scalar::ConstantExpr(constant) => Ok(EvalNode::Constant {
                value: constant.value.clone(),
                data_type: constant.data_type.clone(),
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
                let args: Vec<EvalNode<VectorID>> = func
                    .arguments
                    .iter()
                    .map(Self::eval_scalar)
                    .collect::<Result<_>>()?;
                let arg_types: Vec<&DataTypeImpl> = func.arg_types.iter().collect();
                let func = FunctionFactory::instance().get(func.func_name.as_str(), &arg_types)?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::CastExpr(cast) => {
                let arg = Self::eval_scalar(&cast.argument)?;
                let func = CastFunction::create_try(
                    "",
                    cast.target_type.name().as_str(),
                    cast.from_type.clone(),
                )?;
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
