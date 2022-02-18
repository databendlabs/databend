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
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::aggregates::AggregateFunctionRef;
use once_cell::sync::Lazy;

use crate::plan_expression_common::ExpressionDataTypeVisitor;
use crate::ExpressionVisitor;
use crate::PlanNode;

static OP_SET: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    ["database", "version", "current_user"]
        .iter()
        .copied()
        .collect()
});

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct ExpressionPlan {
    pub exprs: Vec<Expression>,
    pub schema: DataSchemaRef,
    pub input: Arc<PlanNode>,
    pub desc: String,
}

impl ExpressionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum Expression {
    /// An expression with a alias name.
    Alias(String, Box<Expression>),

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
        data_type: DataTypePtr,
    },

    /// A unary expression such as "NOT foo"
    UnaryExpression { op: String, expr: Box<Expression> },

    /// A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },

    /// ScalarFunction with a set of arguments.
    /// Note: BinaryFunction is a also kind of functions function
    ScalarFunction { op: String, args: Vec<Expression> },

    /// AggregateFunction with a set of arguments.
    AggregateFunction {
        op: String,
        distinct: bool,
        params: Vec<DataValue>,
        args: Vec<Expression>,
    },

    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<Expression>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
        /// The original expression from parser. Because sort 'expr' field maybe overwritten by a Column expression, like
        /// from BinaryExpression { +, number, number} to Column(number+number), the orig_expr is for keeping the original
        /// one that is before overwritten. This field is mostly for function monotonicity optimization purpose.
        origin_expr: Box<Expression>,
    },

    /// All fields(*) in a schema.
    Wildcard,

    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expression>,
        /// The `DataType` the expression will yield
        data_type: DataTypePtr,
        /// default false, if it's try_cast, is_null is true
        is_nullable: bool,
    },

    /// Scalar sub query. such as `SELECT (SELECT 1)`
    ScalarSubquery {
        name: String,
        query_plan: Arc<PlanNode>,
    },

    Subquery {
        name: String,
        query_plan: Arc<PlanNode>,
    },
}

impl Expression {
    pub fn create_literal(value: DataValue) -> Expression {
        let data_type = value.data_type();
        Expression::Literal {
            value,
            column_name: None,
            data_type,
        }
    }

    pub fn create_literal_with_type(value: DataValue, data_type: DataTypePtr) -> Expression {
        Expression::Literal {
            value,
            data_type,
            column_name: None,
        }
    }

    pub fn column_name(&self) -> String {
        match self {
            Expression::Alias(name, _expr) => name.clone(),
            Expression::Column(name) => name.clone(),
            Expression::Literal {
                value, column_name, ..
            } => match column_name {
                Some(name) => name.clone(),
                None => {
                    if let DataValue::String(v) = value {
                        match std::str::from_utf8(v) {
                            Ok(v) => format!("'{}'", v),
                            Err(_e) => format!("{:?}", value),
                        }
                    } else {
                        format!("{:?}", value)
                    }
                }
            },
            Expression::UnaryExpression { op, expr } => {
                format!("({} {})", op.to_lowercase(), expr.column_name())
            }
            Expression::BinaryExpression { op, left, right } => {
                format!(
                    "({} {} {})",
                    left.column_name(),
                    op.to_lowercase(),
                    right.column_name()
                )
            }
            Expression::ScalarFunction { op, args } => {
                match OP_SET.get(&op.to_lowercase().as_ref()) {
                    Some(_) => format!("{}()", op),
                    None => {
                        let args_column_name =
                            args.iter().map(Expression::column_name).collect::<Vec<_>>();

                        format!("{}({})", op, args_column_name.join(", "))
                    }
                }
            }
            Expression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let args_column_name = args.iter().map(Expression::column_name).collect::<Vec<_>>();
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
            Expression::Sort { expr, .. } => expr.column_name(),
            Expression::Cast {
                expr, data_type, ..
            } => {
                format!("cast({} as {:?})", expr.column_name(), data_type)
            }
            Expression::Subquery { name, .. } => name.clone(),
            Expression::ScalarSubquery { name, .. } => name.clone(),
            _ => format!("{:?}", self),
        }
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> Result<DataField> {
        let name = self.column_name();
        self.to_data_type(input_schema)
            .map(|return_type| DataField::new(&name, return_type))
    }

    pub fn nullable(&self, input_schema: &DataSchemaRef) -> Result<bool> {
        Ok(self.to_data_type(input_schema)?.is_nullable())
    }

    #[inline(always)]
    pub fn subquery_nullable(subquery_plan: &PlanNode) -> Result<bool> {
        let subquery_schema = subquery_plan.schema();
        let nullable = subquery_schema
            .fields()
            .iter()
            .any(|field| field.is_nullable());
        Ok(nullable)
    }

    pub fn to_subquery_type(subquery_plan: &PlanNode) -> DataTypePtr {
        let subquery_schema = subquery_plan.schema();
        // TODO: This may be wrong.
        let mut columns_field = Vec::with_capacity(subquery_schema.fields().len());

        for column_field in subquery_schema.fields() {
            let ty = ArrayType::create(column_field.data_type().clone());
            columns_field.push(DataField::new(column_field.name(), Arc::new(ty)));
        }

        match columns_field.len() {
            1 => columns_field[0].data_type().clone(),
            _ => {
                let names = columns_field.iter().map(|f| f.name().to_owned()).collect();
                let types = columns_field
                    .iter()
                    .map(|f| f.data_type().to_owned())
                    .collect();

                Arc::new(StructType::create(names, types))
            }
        }
    }

    pub fn to_scalar_subquery_type(subquery_plan: &PlanNode) -> DataTypePtr {
        let subquery_schema = subquery_plan.schema();

        match subquery_schema.fields().len() {
            1 => subquery_schema.field(0).data_type().clone(),
            _ => {
                let names = subquery_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect();
                let types = subquery_schema
                    .fields()
                    .iter()
                    .map(|f| f.data_type().to_owned())
                    .collect();

                Arc::new(StructType::create(names, types))
            }
        }
    }

    pub fn to_data_type(&self, input_schema: &DataSchemaRef) -> Result<DataTypePtr> {
        let visitor = ExpressionDataTypeVisitor::create(input_schema.clone());
        visitor.visit(self)?.finalize()
    }

    pub fn to_aggregate_function(&self, schema: &DataSchemaRef) -> Result<AggregateFunctionRef> {
        match self {
            Expression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let mut func_name = op.clone();
                if *distinct {
                    func_name += "Distinct";
                }

                let mut fields = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    fields.push(arg.to_data_field(schema)?);
                }
                AggregateFunctionFactory::instance().get(&func_name, params.clone(), fields)
            }
            _ => Err(ErrorCode::LogicalError(
                "Expression must be aggregated function",
            )),
        }
    }

    pub fn to_aggregate_function_names(&self) -> Result<Vec<String>> {
        match self {
            Expression::AggregateFunction { args, .. } => {
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

    pub fn create_scalar_function(op: &str, args: Expressions) -> Expression {
        let op = op.to_string();
        Expression::ScalarFunction { op, args }
    }

    pub fn create_unary_expression(op: &str, mut args: Expressions) -> Expression {
        let op = op.to_string();
        let expr = Box::new(args.remove(0));
        Expression::UnaryExpression { op, expr }
    }

    pub fn create_binary_expression(op: &str, mut args: Expressions) -> Expression {
        let op = op.to_string();
        let left = Box::new(args.remove(0));
        let right = Box::new(args.remove(0));
        Expression::BinaryExpression { op, left, right }
    }
}

// Also used as expression column name
impl fmt::Debug for Expression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expression::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            Expression::Column(ref v) => write!(f, "{:#}", v),
            Expression::QualifiedColumn(v) => write!(f, "{:?}", v.join(".")),
            Expression::Literal { ref value, .. } => write!(f, "{:#}", value),
            Expression::Subquery { name, .. } => write!(f, "subquery({})", name),
            Expression::ScalarSubquery { name, .. } => write!(f, "scalar subquery({})", name),
            Expression::BinaryExpression { op, left, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }

            Expression::UnaryExpression { op, expr } => {
                write!(f, "({} {:?})", op, expr)
            }

            Expression::ScalarFunction { op, args } => {
                write!(f, "{}(", op)?;

                for (i, _) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            Expression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let args_column_name = args.iter().map(Expression::column_name).collect::<Vec<_>>();
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

            Expression::Sort { expr, .. } => write!(f, "{:?}", expr),
            Expression::Wildcard => write!(f, "*"),
            Expression::Cast {
                expr, data_type, ..
            } => {
                write!(f, "cast({:?} as {:?})", expr, data_type)
            }
        }
    }
}

pub type Expressions = Vec<Expression>;
