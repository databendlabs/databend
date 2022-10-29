// Copyright 2021 Datafuse Labs.
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

use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_planner::Expression;

use crate::executor::util::format_field_name;
use crate::plans::Scalar;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

pub trait FiledNameFormat {
    fn format(display_name: &str, index: IndexType) -> String;
}

impl FiledNameFormat for ExpressionBuilder<true> {
    fn format(display_name: &str, index: IndexType) -> String {
        format_field_name(display_name, index)
    }
}

impl FiledNameFormat for ExpressionBuilder<false> {
    fn format(display_name: &str, _index: IndexType) -> String {
        display_name.to_owned()
    }
}

pub struct ExpressionBuilder<const FORMAT_WITH_INDEX: bool> {
    metadata: MetadataRef,
}

pub type ExpressionBuilderWithoutRenaming = ExpressionBuilder<false>;
pub type ExpressionBuilderWithRenaming = ExpressionBuilder<true>;

impl<const T: bool> ExpressionBuilder<T>
where ExpressionBuilder<T>: FiledNameFormat
{
    pub fn create(metadata: MetadataRef) -> Self {
        ExpressionBuilder { metadata }
    }

    pub fn build(&self, scalar: &Scalar) -> Result<Expression> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => {
                let metadata = self.metadata.read();
                let name = metadata.column(column_ref.column.index).name();
                Ok(Expression::IndexedVariable {
                    name: name.to_string(),
                    data_type: (*column_ref.column.data_type).clone(),
                })
            }
            Scalar::ConstantExpr(constant) => Ok(Expression::Constant {
                value: constant.value.clone(),
                data_type: *constant.data_type.clone(),
            }),
            Scalar::AndExpr(and) => Ok(Expression::Function {
                name: "and".to_string(),
                args: vec![self.build(&and.left)?, self.build(&and.right)?],
                return_type: and.data_type(),
            }),
            Scalar::OrExpr(or) => Ok(Expression::Function {
                name: "or".to_string(),
                args: vec![self.build(&or.left)?, self.build(&or.right)?],
                return_type: or.data_type(),
            }),
            Scalar::ComparisonExpr(comp) => Ok(Expression::Function {
                name: comp.op.to_func_name(),
                args: vec![self.build(&comp.left)?, self.build(&comp.right)?],
                return_type: comp.data_type(),
            }),
            Scalar::FunctionCall(func) => Ok(Expression::Function {
                name: func.func_name.clone(),
                args: func
                    .arguments
                    .iter()
                    .zip(func.arg_types.iter())
                    .map(|(arg, _)| self.build(arg))
                    .collect::<Result<_>>()?,
                return_type: *func.return_type.clone(),
            }),
            Scalar::CastExpr(cast) => Ok(Expression::Cast {
                input: Box::new(self.build(&cast.argument)?),
                target: *cast.target_type.clone(),
            }),

            _ => Err(ErrorCode::LogicalError(format!(
                "Unsupported physical scalar: {:?}",
                scalar
            ))),
        }
    }

    // the datatype may be wrong if the expression is push down from the upper node (join)
    // todo(leisky)
    pub fn normalize_schema(expression: &Expression, schema: &DataSchema) -> Result<Expression> {
        match expression {
            Expression::IndexedVariable { name, .. } => {
                let data_type = match schema.field_with_name(name) {
                    Ok(f) => f.data_type().clone(),
                    Err(_) => return Ok(expression.clone()),
                };
                Ok(Expression::IndexedVariable {
                    name: name.clone(),
                    data_type,
                })
            }
            Expression::Function { name, args, .. } => {
                let args = args
                    .iter()
                    .map(|arg| Self::normalize_schema(arg, schema))
                    .collect::<Result<Vec<_>>>();

                let args = args?;

                let types = args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
                let types = types.iter().collect::<Vec<_>>();
                let func = FunctionFactory::instance().get(name, &types)?;

                Ok(Expression::Function {
                    name: name.clone(),
                    args,
                    return_type: func.return_type(),
                })
            }

            Expression::Cast { input, target } => Ok(Expression::Cast {
                input: Box::new(Self::normalize_schema(input.as_ref(), schema)?),
                target: target.clone(),
            }),
            Expression::Constant { .. } => Ok(expression.clone()),
        }
    }
}

pub trait ExpressionOp {
    fn binary_op(&self, name: &str, other: &Self) -> Result<Expression>;

    fn and(&self, other: &Self) -> Result<Expression> {
        self.binary_op("and", other)
    }

    fn or(&self, other: &Self) -> Result<Expression> {
        self.binary_op("or", other)
    }

    fn eq(&self, other: &Self) -> Result<Expression> {
        self.binary_op("=", other)
    }

    fn not_eq(&self, other: &Self) -> Result<Expression> {
        self.binary_op("!=", other)
    }

    fn gt_eq(&self, other: &Self) -> Result<Expression> {
        self.binary_op(">=", other)
    }

    fn gt(&self, other: &Self) -> Result<Expression> {
        self.binary_op(">", other)
    }

    fn lt_eq(&self, other: &Self) -> Result<Expression> {
        self.binary_op("<=", other)
    }

    fn lt(&self, other: &Self) -> Result<Expression> {
        self.binary_op("<", other)
    }
}

impl ExpressionOp for Expression {
    fn binary_op(&self, name: &str, other: &Expression) -> Result<Expression> {
        let func =
            FunctionFactory::instance().get(name, &[&self.data_type(), &other.data_type()])?;

        Ok(Expression::Function {
            name: name.to_owned(),
            args: vec![self.clone(), other.clone()],
            return_type: func.return_type(),
        })
    }
}
