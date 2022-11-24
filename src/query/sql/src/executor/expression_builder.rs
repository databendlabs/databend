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

use common_catalog::plan::Expression;
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::NullType;
use common_datavalues::StringType;
use common_datavalues::ToDataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;

use crate::executor::util::format_field_name;
use crate::plans::Scalar;
use crate::ColumnEntry;
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
                let column_entry = metadata.column(column_ref.column.index);
                let name = match column_entry {
                    ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                    ColumnEntry::DerivedColumn { alias, .. } => alias,
                };
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

            _ => Err(ErrorCode::Internal(format!(
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

    fn unary_op(&self, name: &str) -> Result<Expression>;

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

    fn unary_op(&self, name: &str) -> Result<Expression> {
        let func = FunctionFactory::instance().get(name, &[&self.data_type()])?;

        Ok(Expression::Function {
            name: name.to_owned(),
            args: vec![self.clone()],
            return_type: func.return_type(),
        })
    }
}

/// Add binary function.
pub fn add(left: Expression, right: Expression) -> Expression {
    left.binary_op("+", &right).unwrap()
}

/// Sub binary function.
pub fn sub(left: Expression, right: Expression) -> Expression {
    left.binary_op("-", &right).unwrap()
}

/// Not.
pub fn not(other: Expression) -> Expression {
    other.unary_op("not").unwrap()
}

// Neg.
pub fn neg(other: Expression) -> Expression {
    other.unary_op("negate").unwrap()
}

/// Mod binary function.
pub fn modular(left: Expression, right: Expression) -> Expression {
    left.binary_op("%", &right).unwrap()
}

/// sum() aggregate function.
pub fn sum(other: Expression) -> Expression {
    other.unary_op("sum").unwrap()
}

/// avg() aggregate function.
pub fn avg(other: Expression) -> Expression {
    other.unary_op("avg").unwrap()
}

pub fn func(name: &str) -> Result<Expression> {
    let func = FunctionFactory::instance().get(name, &[])?;

    Ok(Expression::Function {
        name: name.to_owned(),
        args: vec![],
        return_type: func.return_type(),
    })
}

pub fn col(name: &str, data_type: DataTypeImpl) -> Expression {
    Expression::IndexedVariable {
        name: name.to_string(),
        data_type,
    }
}

pub trait Literal {
    fn to_literal(&self) -> Expression;
}

impl Literal for &[u8] {
    fn to_literal(&self) -> Expression {
        Expression::Constant {
            value: DataValue::String(self.to_vec()),
            data_type: *Box::new(StringType::new_impl()),
        }
    }
}

impl Literal for Vec<u8> {
    fn to_literal(&self) -> Expression {
        Expression::Constant {
            value: DataValue::String(self.clone()),
            data_type: *Box::new(StringType::new_impl()),
        }
    }
}

macro_rules! make_literal {
    ($TYPE: ty, $SUPER: ident, $SCALAR: ident) => {
        #[allow(missing_docs)]
        impl Literal for $TYPE {
            fn to_literal(&self) -> Expression {
                Expression::Constant {
                    value: DataValue::$SCALAR(*self as $SUPER),
                    data_type: *Box::new($SUPER::to_data_type()),
                }
            }
        }
    };
}

make_literal!(bool, bool, Boolean);
make_literal!(f32, f64, Float64);
make_literal!(f64, f64, Float64);

make_literal!(i8, i64, Int64);
make_literal!(i16, i64, Int64);
make_literal!(i32, i64, Int64);
make_literal!(i64, i64, Int64);

make_literal!(u8, u64, UInt64);
make_literal!(u16, u64, UInt64);
make_literal!(u32, u64, UInt64);
make_literal!(u64, u64, UInt64);

pub fn lit<T: Literal>(n: T) -> Expression {
    n.to_literal()
}

pub fn lit_null() -> Expression {
    Expression::Constant {
        value: DataValue::Null,
        data_type: *Box::new(NullType::new_impl()),
    }
}
