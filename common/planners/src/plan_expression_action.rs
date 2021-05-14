// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::{Result, ErrorCodes};
use common_functions::AliasFunction;
use common_functions::CastFunction;
use common_functions::ColumnFunction;
use common_functions::FunctionFactory;
use common_functions::IFunction;
use common_functions::LiteralFunction;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum ExpressionAction {
    /// An expression with a alias name.
    Alias(String, Box<ExpressionAction>),
    /// Column name.
    Column(String),
    /// Constant value.
    Literal(DataValue),
    /// A binary expression such as "age > 40"
    BinaryExpression {
        left: Box<ExpressionAction>,
        op: String,
        right: Box<ExpressionAction>
    },
    /// Functions with a set of arguments.
    Function {
        op: String,
        args: Vec<ExpressionAction>
    },

    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<ExpressionAction>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool
    },
    /// All fields(*) in a schema.
    Wildcard,
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<ExpressionAction>,
        /// The `DataType` the expression will yield
        data_type: DataType
    }
}

impl ExpressionAction {
    fn to_function_with_depth(&self, depth: usize) -> Result<Box<dyn IFunction>> {
        match self {
            ExpressionAction::Column(ref v) => ColumnFunction::try_create(v.as_str()),
            ExpressionAction::Literal(ref v) => LiteralFunction::try_create(v.clone()),
            ExpressionAction::BinaryExpression { left, op, right } => {
                let l = left.to_function_with_depth(depth)?;
                let r = right.to_function_with_depth(depth + 1)?;
                let mut func = FunctionFactory::get(op, &[l, r])?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionAction::Function { op, args } => {
                let mut funcs = Vec::with_capacity(args.len());
                for arg in args {
                    let mut func = arg.to_function_with_depth(depth + 1)?;
                    func.set_depth(depth);
                    funcs.push(func);
                }
                let mut func = FunctionFactory::get(op, &funcs)?;
                func.set_depth(depth);
                Ok(func)
            }
            ExpressionAction::Alias(alias, expr) => {
                let mut func = expr.to_function_with_depth(depth)?;
                func.set_depth(depth);
                AliasFunction::try_create(alias.clone(), func)
            }
            ExpressionAction::Sort { expr, .. } => expr.to_function_with_depth(depth),
            ExpressionAction::Wildcard => ColumnFunction::try_create("*"),
            ExpressionAction::Cast { expr, data_type } => Ok(CastFunction::create(
                expr.to_function_with_depth(depth)?,
                data_type.clone()
            )),
        }
    }

    pub fn to_function(&self) -> Result<Box<dyn IFunction>> {
        self.to_function_with_depth(0)
    }

    // TODO fixme: create IAggregateFunction
    pub fn to_aggregate_function(&self) -> Result<Box<dyn IFunction>> {
        self.to_function_with_depth(0)
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> Result<DataField> {
        self.to_function().and_then(|function| {
            function.return_type(&input_schema).and_then(|return_type| {
                function.nullable(&input_schema).map(|nullable| {
                    DataField::new(format!("{}", function).as_str(), return_type, nullable)
                })
            })
        })
    }

    pub fn has_aggregator(&self) -> Result<bool> {
        Ok(self.to_function()?.has_aggregator())
    }
}

impl fmt::Debug for ExpressionAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionAction::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            ExpressionAction::Column(ref v) => write!(f, "{:#}", v),
            ExpressionAction::Literal(ref v) => write!(f, "{:#}", v),
            ExpressionAction::BinaryExpression { left, op, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }
            ExpressionAction::Function { op, args } => write!(f, "{}({:?})", op, args),
            ExpressionAction::Sort { expr, .. } => write!(f, "{:?}", expr),
            ExpressionAction::Wildcard => write!(f, "*"),
            ExpressionAction::Cast { expr, data_type } => {
                write!(f, "cast({:?} as {:?})", expr, data_type)
            }
        }
    }
}
