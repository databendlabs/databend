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

use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionContext;
use common_functions::scalars::FunctionFactory;

use super::eval_context::EmptyEvalContext;
use crate::common::evaluator::eval_context::EvalContext;
use crate::common::evaluator::eval_context::TypedVector;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::IndexType;

enum EvalNode {
    Function {
        func: Box<dyn Function>,
        args: Vec<EvalNode>,
    },
    Const {
        constant: DataValue,
        data_type: DataTypeImpl,
    },
    Variable {
        index: IndexType,
    },
}

pub struct ScalarEvaluator {
    eval_tree: EvalNode,
}

impl ScalarEvaluator {
    pub fn try_create(scalar: &Scalar) -> Result<Self> {
        let eval_tree = Self::build_eval_tree(scalar)?;
        Ok(Self { eval_tree })
    }

    fn build_eval_tree(scalar: &Scalar) -> Result<EvalNode> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => Ok(EvalNode::Variable {
                index: column_ref.column.index,
            }),
            Scalar::ConstantExpr(constant) => Ok(EvalNode::Const {
                constant: constant.value.clone(),
                data_type: constant.data_type.clone(),
            }),
            Scalar::AndExpr(and) => {
                let args = vec![
                    Self::build_eval_tree(&and.left)?,
                    Self::build_eval_tree(&and.right)?,
                ];
                let func = FunctionFactory::instance()
                    .get("and", &[&and.left.data_type(), &and.right.data_type()])?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::OrExpr(or) => {
                let args = vec![
                    Self::build_eval_tree(&or.left)?,
                    Self::build_eval_tree(&or.right)?,
                ];
                let func = FunctionFactory::instance()
                    .get("or", &[&or.left.data_type(), &or.right.data_type()])?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::ComparisonExpr(comp) => {
                let args = vec![
                    Self::build_eval_tree(&comp.left)?,
                    Self::build_eval_tree(&comp.right)?,
                ];
                let func = FunctionFactory::instance().get(comp.op.to_func_name(), &[
                    &comp.left.data_type(),
                    &comp.right.data_type(),
                ])?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::FunctionCall(func) => {
                let args: Vec<EvalNode> = func
                    .arguments
                    .iter()
                    .map(Self::build_eval_tree)
                    .collect::<Result<_>>()?;
                let arg_types: Vec<&DataTypeImpl> = func.arg_types.iter().collect();
                let func = FunctionFactory::instance().get(func.func_name.as_str(), &arg_types)?;
                Ok(EvalNode::Function { func, args })
            }
            Scalar::CastExpr(cast) => {
                let arg = Self::build_eval_tree(&cast.argument)?;
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

    fn eval_impl(
        &self,
        eval_node: &EvalNode,
        func_ctx: &FunctionContext,
        eval_ctx: &impl EvalContext<VectorID = IndexType>,
    ) -> Result<TypedVector> {
        match &eval_node {
            EvalNode::Function { func, args } => {
                let args = args
                    .iter()
                    .map(|arg| {
                        let vector = self.eval_impl(arg, func_ctx, eval_ctx)?;
                        Ok(ColumnWithField::new(
                            vector.vector().clone(),
                            DataField::new("", vector.logical_type()),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TypedVector::new(
                    func.eval(func_ctx.clone(), &args, eval_ctx.tuple_count())?,
                    func.return_type(),
                ))
            }
            EvalNode::Const {
                constant,
                data_type,
            } => {
                let vector = constant.as_const_column(data_type, eval_ctx.tuple_count())?;
                Ok(TypedVector::new(vector, data_type.clone()))
            }
            EvalNode::Variable { index } => eval_ctx.get_vector(index),
        }
    }

    pub fn eval(
        &self,
        func_ctx: &FunctionContext,
        eval_ctx: &impl EvalContext<VectorID = IndexType>,
    ) -> Result<TypedVector> {
        self.eval_impl(&self.eval_tree, func_ctx, eval_ctx)
    }

    pub fn try_eval_const(&self, func_ctx: &FunctionContext) -> Result<(DataValue, DataTypeImpl)> {
        let eval_ctx = EmptyEvalContext;
        let vector = self.eval_impl(&self.eval_tree, func_ctx, &eval_ctx)?;
        if vector.vector().is_const() {
            Ok((vector.vector().get(0), vector.logical_type()))
        } else {
            Err(ErrorCode::LogicalError(
                "Non-constant column can not be evaluated by try_eval_const",
            ))
        }
    }
}
