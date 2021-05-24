// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_aggregate_functions::AggregateFunctionFactory;
use common_aggregate_functions::IAggreagteFunction;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::FunctionFactory;

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

    /// ScalarFunction with a set of arguments.
    /// Note: BinaryFunction is a also kind of functions function
    ScalarFunction {
        op: String,
        args: Vec<ExpressionAction>
    },

    /// AggregateFunction with a set of arguments.
    AggregateFunction {
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
    pub fn column_name(&self) -> String {
        match self {
            ExpressionAction::Alias(name, _expr) => name.clone(),
            _ => format!("{:?}", self)
        }
    }

    pub fn to_data_field(&self, input_schema: &DataSchemaRef) -> Result<DataField> {
        let name = self.column_name();
        self.to_data_type(&input_schema).and_then(|return_type| {
            self.nullable(&input_schema)
                .map(|nullable| DataField::new(&name, return_type, nullable))
        })
    }

    // TODO
    pub fn nullable(&self, _input_schema: &DataSchemaRef) -> Result<bool> {
        Ok(false)
    }

    pub fn to_data_type(&self, input_schema: &DataSchemaRef) -> Result<DataType> {
        match self {
            ExpressionAction::Alias(_, expr) => expr.to_data_type(input_schema),
            ExpressionAction::Column(s) => Ok(input_schema.field_with_name(s)?.data_type().clone()),
            ExpressionAction::Literal(v) => Ok(v.data_type()),
            ExpressionAction::BinaryExpression { op, left, right } => {
                let arg_types = vec![
                    left.to_data_type(input_schema)?,
                    right.to_data_type(input_schema)?,
                ];
                let func = FunctionFactory::get(op)?;
                func.return_type(&arg_types)
            }
            ExpressionAction::ScalarFunction { op, args } => {
                let mut arg_types = Vec::with_capacity(args.len());
                for arg in args {
                    arg_types.push(arg.to_data_type(input_schema)?);
                }
                let func = FunctionFactory::get(op)?;
                func.return_type(&arg_types)
            }
            ExpressionAction::AggregateFunction { op, args } => {
                let mut arg_types = Vec::with_capacity(args.len());
                for arg in args {
                    arg_types.push(arg.to_data_type(input_schema)?);
                }
                let func = AggregateFunctionFactory::get(op)?;
                func.return_type(&arg_types)
            }
            ExpressionAction::Wildcard => Result::Err(ErrorCodes::IllegalDataType(
                "Wildcard expressions are not valid to get return type"
            )),
            ExpressionAction::Cast { data_type, .. } => Ok(data_type.clone()),
            ExpressionAction::Sort { expr, .. } => expr.to_data_type(input_schema)
        }
    }

    pub fn to_aggregate_function(&self) -> Result<Box<dyn IAggreagteFunction>> {
        match self {
            ExpressionAction::AggregateFunction { op, .. } => AggregateFunctionFactory::get(op),
            _ => Err(ErrorCodes::LogicalError(
                "Expression must be aggregated function"
            ))
        }
    }

    pub fn to_aggregate_function_args(&self) -> Result<Vec<String>> {
        match self {
            ExpressionAction::AggregateFunction { args, .. } => {
                let arg_names = args.iter().map(|arg| arg.column_name()).collect::<Vec<_>>();
                Ok(arg_names)
            }
            _ => Err(ErrorCodes::LogicalError(
                "Expression must be aggregated function"
            ))
        }
    }

    pub fn to_debug_str(&self) -> String {
        match self {
            ExpressionAction::BinaryExpression { .. } => "BIN()".to_string(),
            ExpressionAction::Sort { expr, .. } => {
                format!("SORT({})", expr.column_name())
            }
            ExpressionAction::ScalarFunction { .. } => "SC()".to_string(),
            _ => "other".to_string()
        }
    }
}

// Also used as expression column name
impl fmt::Debug for ExpressionAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionAction::Alias(alias, v) => write!(f, "{:?} as {:#}", v, alias),
            ExpressionAction::Column(ref v) => write!(f, "{:#}", v),
            ExpressionAction::Literal(ref v) => write!(f, "{:#}", v),
            ExpressionAction::BinaryExpression { op, left, right } => {
                write!(f, "({:?} {} {:?})", left, op, right,)
            }
            ExpressionAction::ScalarFunction { op, args } => {
                write!(f, "{}(", op)?;

                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            ExpressionAction::AggregateFunction { op, args } => {
                write!(f, "{}(", op)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i],)?;
                }
                write!(f, ")")
            }

            ExpressionAction::Sort { expr, .. } => write!(f, "{:?}", expr),
            ExpressionAction::Wildcard => write!(f, "*"),
            ExpressionAction::Cast { expr, data_type } => {
                write!(f, "cast({:?} as {:?})", expr, data_type)
            }
        }
    }
}
