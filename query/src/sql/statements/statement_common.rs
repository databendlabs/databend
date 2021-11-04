use common_exception::{Result, ErrorCode};
use sqlparser::ast::{Expr, Select, UnaryOperator, FunctionArg, Value, Ident, BinaryOperator, Query, DataType};
use common_datavalues::{DataSchema, DataValue, DataSchemaRef};
use common_planners::Expression;
use crate::functions::ContextFunction;
use common_functions::aggregates::AggregateFunctionFactory;
use crate::sql::SQLCommon;
use crate::sessions::DatabendQueryContextRef;

struct StatementCommon;

/// Such as `SELECT * FROM table_name AS alias_name`
pub enum TableSchema {
    Named(String, DataSchemaRef),
    UnNamed(DataSchemaRef),
    Default { database: String, table: String, schema: DataSchemaRef },
}

pub type TablesSchema = Vec<TableSchema>;

pub struct ExpressionAnalyzer {
    tables: TablesSchema,
    ctx: DatabendQueryContextRef,
}

impl ExpressionAnalyzer {
    pub fn create(ctx: DatabendQueryContextRef) -> ExpressionAnalyzer {
        ExpressionAnalyzer { ctx, ..Default::default() }
    }

    pub fn with_tables(ctx: DatabendQueryContextRef, tables: TablesSchema) -> ExpressionAnalyzer {
        ExpressionAnalyzer { ctx, tables }
    }

    pub fn analyze(&self, expr: &Expr) -> Result<Expression> {
        match expr {
            Expr::Value(value) => self.analyze_value(value),
            Expr::Identifier(ref v) => self.analyze_identifier(v),
            Expr::BinaryOp { left, op, right } => self.analyze_binary_expr(left, op, right),
            Expr::UnaryOp { op, expr } => self.analyze_unary_expr(op, expr),
            Expr::IsNull(expr) => self.analyze_is_null(expr),
            Expr::IsNotNull(expr) => self.analyze_is_not_null(expr),
            Expr::Nested(expr) => self.analyze(expr),
            Expr::Exists(subquery) => self.analyze_exists(subquery),
            Expr::Subquery(q) => Ok(self.scalar_subquery_to_rex(q)?),
            Expr::CompoundIdentifier(ids) => {
                self.process_compound_ident(ids.as_slice())
            }
            Expr::Function(e) => {
                let mut args = Vec::with_capacity(e.args.len());

                // 1. Get the args from context by function name. such as SELECT database()
                // common::ScalarFunctions::udf::database arg is ctx.get_default()
                let ctx_args = ContextFunction::build_args_from_ctx(
                    e.name.to_string().as_str(),
                    self.ctx.clone(),
                )?;
                if !ctx_args.is_empty() {
                    args.extend_from_slice(ctx_args.as_slice());
                }

                // 2. Get args from the ast::Expr:Function
                for arg in &e.args {
                    match &arg {
                        FunctionArg::Named { arg, .. } => {
                            args.push(Self::sql_to_rex(arg, schema)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            args.push(Self::sql_to_rex(arg, schema)?);
                        }
                    }
                }

                let op = e.name.to_string();
                if AggregateFunctionFactory::instance().check(&op) {
                    let args = match op.to_lowercase().as_str() {
                        "count" => args
                            .iter()
                            .map(|c| match c {
                                Expression::Wildcard => common_planners::lit(0i64),
                                _ => c.clone(),
                            })
                            .collect(),
                        _ => args,
                    };

                    let params = e
                        .params
                        .iter()
                        .map(|v| {
                            let expr = Self::value_to_rex(v);
                            if let Ok(Expression::Literal { value, .. }) = expr {
                                Ok(value)
                            } else {
                                Result::Err(ErrorCode::SyntaxException(format!(
                                    "Unsupported value expression: {:?}, must be datavalue",
                                    expr
                                )))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    return Ok(Expression::AggregateFunction {
                        op,
                        distinct: e.distinct,
                        params,
                        args,
                    });
                }

                Ok(Expression::ScalarFunction { op, args })
            }
            Expr::Wildcard => self.analyze_wildcard(),
            Expr::TypedString { data_type, value } => self.analyze_typed_string(data_type, value),
            Expr::Cast { expr, data_type } => self.analyze_cast(expr, data_type),
            Expr::Substring { expr, substring_from, substring_for, } => self.analyze_substring(expr, substring_from, substring_for),
            Expr::Between { expr, negated, low, high } => self.analyze_between(expr, negated, low, high),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
        }
    }

    fn analyze_exists(&self, subquery: &Query) -> Result<Expression> {
        Ok(Expression::ScalarFunction {
            op: "EXISTS".to_lowercase(),
            args: vec![self.subquery_to_rex(subquery)?],
        })
    }

    fn analyze_between(&self, expr: &Expr, negated: &bool, low: &Expr, high: &Expr) -> Result<Expression> {
        let expression = self.analyze(expr)?;
        let low_expression = self.analyze(low)?;
        let high_expression = self.analyze(high)?;
        Ok(match *negated {
            false => expression
                .gt_eq(low_expression)
                .and(expression.lt(high_expression)),
            true => expression
                .lt(low_expression)
                .or(expression.gt_eq(high_expression)),
        })
    }

    fn analyze_substring(&self, expr: &Expr, from: &Option<Box<Expr>>, length: &Option<Box<Expr>>) -> Result<Expression> {
        let mut args = Vec::with_capacity(3);
        args.push(Self::sql_to_rex(expr, schema)?);
        if let Some(from) = from {
            args.push(Self::sql_to_rex(from, schema)?);
        } else {
            args.push(Expression::create_literal(DataValue::Int64(Some(1))));
        }

        if let Some(len) = length {
            args.push(Self::sql_to_rex(len, schema)?);
        }

        Ok(Expression::ScalarFunction {
            op: "substring".to_string(),
            args,
        })
    }

    fn analyze_cast(&self, expr: &Expr, data_type: &DataType) -> Result<Expression> {
        let expr = self.analyze(expr)?;
        let cast_to_type = SQLCommon::make_data_type(data_type)?;
        Ok(Expression::Cast { expr: Box::new(expr), data_type: cast_to_type })
    }

    fn analyze_typed_string(&self, data_type: &DataType, value: &str) -> Result<Expression> {
        SQLCommon::make_data_type(data_type).map(|data_type| Expression::Cast {
            expr: Box::new(Expression::create_literal(DataValue::String(Some(
                value.clone().into_bytes(),
            )))),
            data_type,
        })
    }

    fn analyze_wildcard(&self) -> Result<Expression> {
        Ok(Expression::Wildcard)
    }

    fn analyze_binary_expr(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<Expression> {
        Ok(Expression::BinaryExpression {
            op: format!("{}", op),
            left: Box::new(Self::sql_to_rex(left, schema)?),
            right: Box::new(Self::sql_to_rex(right, schema)?),
        })
    }

    fn analyze_unary_expr(&self, op: &UnaryOperator, expr: &Expr) -> Result<Expression> {
        match op {
            UnaryOperator::Plus => self.analyze(expr),
            _other_operator => Ok(Expression::UnaryExpression {
                op: format!("{}", op),
                expr: Box::new(self.analyze(expr)?),
            }),
        }
    }

    fn analyze_identifier(&self, v: &Ident) -> Result<Expression> {
        Ok(Expression::Column(v.clone().value))
    }

    fn analyze_is_null(&self, expr: &Expr) -> Result<Expression> {
        Ok(Expression::ScalarFunction {
            op: String::from("is_null"),
            args: vec![self.analyze(expr)?],
        })
    }

    fn analyze_is_not_null(&self, expr: &Expr) -> Result<Expression> {
        Ok(Expression::ScalarFunction {
            op: String::from("isnotnull"),
            args: vec![self.analyze(expr)?],
        })
    }
}

struct ValueAnalyzer;

impl ValueAnalyzer {
    pub fn analyze(value: &Value) -> Result<Expression> {}
}

impl StatementCommon {
    fn value_to_rex(value: &Value) -> Result<Expression> {
        match value {
            Value::Number(ref n, _) => {
                DataValue::try_from_literal(n).map(Expression::create_literal)
            }
            Value::SingleQuotedString(ref value) => {
                Ok(Expression::create_literal(
                    DataValue::String(Some(value.clone().into_bytes())),
                ))
            }
            Value::Boolean(b) => {
                Ok(Expression::create_literal(DataValue::Boolean(Some(*b))))
            }
            Value::Interval {
                value: value_expr,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                // We don't support full interval expression like 'Interval ..To.. '. Currently only partial interval expression like "interval [num] [unit]" is supported.
                if leading_precision.is_some()
                    || last_field.is_some()
                    || fractional_seconds_precision.is_some()
                {
                    return Result::Err(ErrorCode::SyntaxException(format!(
                        "Unsupported interval expression: {}.",
                        value
                    )));
                }

                // When the input is like "interval '1 hour'", leading_field will be None and value_expr will be '1 hour'.
                // We may want to support this pattern in native paser (sqlparser-rs), to have a parsing result that leading_field is Some(Hour) and value_expr is number '1'.
                if leading_field.is_none() {
                    //TODO: support parsing literal interval like '1 hour'
                    return Result::Err(ErrorCode::SyntaxException(format!(
                        "Unsupported interval expression: {}.",
                        value
                    )));
                }
                Self::interval_to_rex(value_expr, leading_field.clone().unwrap())
            }
            Value::Null => Ok(Expression::create_literal(DataValue::Null)),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported value expression: {}, type: {:?}",
                value, other
            ))),
        }
    }
}
