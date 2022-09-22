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

use std::collections::HashSet;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use once_cell::sync::Lazy;

use crate::plan_expression_common::ExpressionDataTypeVisitor;
use crate::ExpressionVisitor;

static OP_SET: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    ["database", "version", "current_user", "user"]
        .iter()
        .copied()
        .collect()
});

/// REMOVE ME: LegacyExpression should be removed.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum LegacyExpression {
    /// An expression with a alias name.
    Alias(String, Box<LegacyExpression>),

    /// Column name.
    Column(String),
    /// Qualified column name.
    QualifiedColumn(Vec<String>),

    /// Constant value.
    /// Note: When literal represents a column, its column_name will not be None
    Literal {
        value: DataValue,
        column_name: Option<String>,

        // Logic data_type for this literal
        data_type: DataTypeImpl,
    },

    /// A unary expression such as "NOT foo"
    UnaryExpression {
        op: String,
        expr: Box<LegacyExpression>,
    },

    /// A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<LegacyExpression>,
        op: String,
        right: Box<LegacyExpression>,
    },

    /// ScalarFunction with a set of arguments.
    /// Note: BinaryFunction is a also kind of functions function
    ScalarFunction {
        op: String,
        args: Vec<LegacyExpression>,
    },

    /// AggregateFunction with a set of arguments.
    AggregateFunction {
        op: String,
        distinct: bool,
        params: Vec<DataValue>,
        args: Vec<LegacyExpression>,
    },

    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<LegacyExpression>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
        /// The original expression from parser. Because sort 'expr' field maybe overwritten by a Column expression, like
        /// from BinaryExpression { +, number, number} to Column(number+number), the orig_expr is for keeping the original
        /// one that is before overwritten. This field is mostly for function monotonicity optimization purpose.
        origin_expr: Box<LegacyExpression>,
    },

    /// All fields(*) in a schema.
    Wildcard,

    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<LegacyExpression>,
        /// The `DataType` the expression will yield
        data_type: DataTypeImpl,
        /// The PostgreSQL style cast `expr::datatype`
        pg_style: bool,
    },

    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0][1]`, or `obj:k1:k2`
    MapAccess {
        name: String,
        args: Vec<LegacyExpression>,
    },
}

impl LegacyExpression {
    pub fn create_literal(value: DataValue) -> LegacyExpression {
        let data_type = value.data_type();
        LegacyExpression::Literal {
            value,
            column_name: None,
            data_type,
        }
    }

    pub fn create_literal_with_type(value: DataValue, data_type: DataTypeImpl) -> LegacyExpression {
        LegacyExpression::Literal {
            value,
            data_type,
            column_name: None,
        }
    }

    pub fn column_name(&self) -> String {
        match self {
            LegacyExpression::Alias(name, _expr) => name.clone(),
            LegacyExpression::Column(name) => name.clone(),
            LegacyExpression::Literal {
                value, column_name, ..
            } => match column_name {
                Some(name) => name.clone(),
                None => format_datavalue_sql(value),
            },
            LegacyExpression::UnaryExpression { op, expr } => {
                format!("({} {})", op.to_lowercase(), expr.column_name())
            }
            LegacyExpression::BinaryExpression { op, left, right } => {
                format!(
                    "({} {} {})",
                    left.column_name(),
                    op.to_lowercase(),
                    right.column_name()
                )
            }
            LegacyExpression::ScalarFunction { op, args } => {
                match OP_SET.get(&op.to_lowercase().as_ref()) {
                    Some(_) => format!("{}()", op),
                    None => {
                        let args_column_name = args
                            .iter()
                            .map(LegacyExpression::column_name)
                            .collect::<Vec<_>>();

                        format!("{}({})", op, args_column_name.join(", "))
                    }
                }
            }
            LegacyExpression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let args_column_name = args
                    .iter()
                    .map(LegacyExpression::column_name)
                    .collect::<Vec<_>>();
                let params_name = params
                    .iter()
                    .map(|v| DataValue::custom_display(v, true))
                    .collect::<Vec<_>>();

                let prefix = if params.is_empty() {
                    op.to_string()
                } else {
                    format!("{}({})", op, params_name.join(", "))
                };

                match distinct {
                    true => format!("{}(distinct {})", prefix, args_column_name.join(", ")),
                    false => format!("{}({})", prefix, args_column_name.join(", ")),
                }
            }
            LegacyExpression::Sort { expr, .. } => expr.column_name(),
            LegacyExpression::Cast {
                expr,
                data_type,
                pg_style,
            } => {
                if *pg_style {
                    format!("{}::{}", expr.column_name(), data_type.sql_name())
                } else if data_type.is_nullable() {
                    let ty: NullableType = data_type.to_owned().try_into().unwrap();
                    format!(
                        "try_cast({} as {})",
                        expr.column_name(),
                        ty.inner_type().sql_name()
                    )
                } else {
                    format!("cast({} as {})", expr.column_name(), data_type.sql_name())
                }
            }
            LegacyExpression::MapAccess { name, .. } => name.clone(),
            _ => format!("{:?}", self),
        }
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> Result<DataField> {
        let name = self.column_name();
        self.to_data_type(input_schema)
            .map(|return_type| DataField::new(&name, return_type))
    }

    pub fn to_data_type(&self, input_schema: &DataSchemaRef) -> Result<DataTypeImpl> {
        let visitor = ExpressionDataTypeVisitor::create(input_schema.clone());
        visitor.visit(self)?.finalize()
    }

    pub fn to_aggregate_function(&self, schema: &DataSchemaRef) -> Result<AggregateFunctionRef> {
        match self {
            LegacyExpression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let mut func_name = op.clone();
                if *distinct {
                    func_name += "_distinct";
                }

                let mut fields = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    fields.push(arg.to_data_field(schema)?);
                }
                AggregateFunctionFactory::instance().get(&func_name, params.clone(), fields)
            }
            _ => Err(ErrorCode::LogicalError(format!(
                "Expression must be aggregated function, {:?}",
                self
            ))),
        }
    }

    pub fn to_aggregate_function_names(&self) -> Result<Vec<String>> {
        match self {
            LegacyExpression::AggregateFunction { args, .. } => {
                let mut names = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    names.push(arg.column_name());
                }
                Ok(names)
            }
            _ => Err(ErrorCode::LogicalError(
                "Expression must be aggregated function",
            )),
        }
    }

    pub fn create_scalar_function(op: &str, args: Expressions) -> LegacyExpression {
        let op = op.to_string();
        LegacyExpression::ScalarFunction { op, args }
    }

    pub fn create_unary_expression(op: &str, mut args: Expressions) -> LegacyExpression {
        let op = op.to_string();
        let expr = Box::new(args.remove(0));
        LegacyExpression::UnaryExpression { op, expr }
    }

    pub fn create_binary_expression(op: &str, mut args: Expressions) -> LegacyExpression {
        let op = op.to_string();
        let left = Box::new(args.remove(0));
        let right = Box::new(args.remove(0));
        LegacyExpression::BinaryExpression { op, left, right }
    }
}

// Also used as expression column name
impl fmt::Debug for LegacyExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LegacyExpression::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            LegacyExpression::Column(ref v) => write!(f, "{:#}", v),
            LegacyExpression::QualifiedColumn(v) => write!(f, "{:?}", v.join(".")),
            LegacyExpression::Literal { ref value, .. } => write!(f, "{:#}", value),
            LegacyExpression::BinaryExpression { op, left, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }

            LegacyExpression::UnaryExpression { op, expr } => {
                write!(f, "({} {:?})", op, expr)
            }

            LegacyExpression::ScalarFunction { op, args } => {
                write!(f, "{}(", op)?;

                for (i, _) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            LegacyExpression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let args_column_name = args
                    .iter()
                    .map(LegacyExpression::column_name)
                    .collect::<Vec<_>>();
                let params_name = params
                    .iter()
                    .map(|v| DataValue::custom_display(v, true))
                    .collect::<Vec<_>>();

                if params.is_empty() {
                    write!(f, "{}", op)?;
                } else {
                    write!(f, "{}({})", op, params_name.join(", "))?;
                };

                match distinct {
                    true => write!(f, "(distinct {})", args_column_name.join(", "))?,
                    false => write!(f, "({})", args_column_name.join(", "))?,
                }
                Ok(())
            }

            LegacyExpression::Sort { expr, .. } => write!(f, "{:?}", expr),
            LegacyExpression::Wildcard => write!(f, "*"),
            LegacyExpression::Cast {
                expr,
                data_type,
                pg_style,
            } => {
                if *pg_style {
                    write!(f, "{:?}::{}", expr, data_type.name())
                } else if data_type.is_nullable() {
                    let ty: NullableType = data_type.to_owned().try_into().unwrap();
                    write!(f, "try_cast({:?} as {})", expr, ty.inner_type().name())
                } else {
                    write!(f, "cast({:?} as {})", expr, data_type.name())
                }
            }
            LegacyExpression::MapAccess { name, .. } => write!(f, "{}", name),
        }
    }
}

pub type Expressions = Vec<LegacyExpression>;
