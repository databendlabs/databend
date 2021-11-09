use common_exception::{Result, ErrorCode};
use sqlparser::ast::{Expr, Select, UnaryOperator, FunctionArg, Value, Ident, BinaryOperator, Query, DataType, DateTimeField, Function};
use common_datavalues::{DataSchema, DataValue, DataSchemaRef, IntervalUnit};
use common_planners::Expression;
use crate::functions::ContextFunction;
use common_functions::aggregates::AggregateFunctionFactory;
use crate::sql::{SQLCommon, DfStatement, PlanParser};
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext};
use crate::sql::statements::analyzer_schema::AnalyzedSchema;
use std::sync::Arc;
use crate::sql::statements::analyzer_expr_value::ValueExprAnalyzer;
use crate::sql::statements::{DfQueryStatement, AnalyzableStatement, AnalyzedResult};
use std::convert::TryFrom;

/// Such as `SELECT * FROM table_name AS alias_name`
pub enum TableSchema {
    Named(String, DataSchemaRef),
    UnNamed(DataSchemaRef),
    Default { database: String, table: String, schema: DataSchemaRef },
}

pub type TablesSchema = Vec<TableSchema>;

pub struct ExpressionAnalyzer {
    schema: Arc<AnalyzedSchema>,
    context: DatabendQueryContextRef,
}

impl ExpressionAnalyzer {
    pub fn create(ctx: DatabendQueryContextRef, allow_aggr: bool) -> ExpressionAnalyzer {
        ExpressionAnalyzer { context: ctx, ..Default::default() }
    }

    pub fn with_tables(ctx: DatabendQueryContextRef, tables: TablesSchema) -> ExpressionAnalyzer {
        ExpressionAnalyzer { context: ctx, ..Default::default() }
    }

    pub fn with_source(ctx: DatabendQueryContextRef, source: Arc<AnalyzedSchema>, allow_aggr: bool) -> ExpressionAnalyzer {}

    pub async fn analyze(&self, expr: &Expr) -> Result<Expression> {
        match expr {
            Expr::Nested(expr) => self.analyze(expr).await,
            Expr::Value(value) => ValueExprAnalyzer::analyze(value),
            Expr::Identifier(ident) => self.analyze_identifier(ident),
            Expr::CompoundIdentifier(idents) => self.analyze_identifiers(idents),
            Expr::IsNull(expr) => self.analyze_is_null(expr).await,
            Expr::IsNotNull(expr) => self.analyze_is_not_null(expr).await,
            Expr::UnaryOp { op, expr } => self.analyze_unary_expr(op, expr).await,
            Expr::BinaryOp { left, op, right } => self.analyze_binary_expr(left, op, right).await,
            Expr::Wildcard => self.analyze_wildcard(),
            Expr::Exists(subquery) => self.analyze_exists(subquery).await,
            Expr::Subquery(subquery) => self.analyze_scalar_subquery(subquery).await,
            Expr::Function(function) => self.analyze_function(function).await,
            Expr::Cast { expr, data_type } => self.analyze_cast(expr, data_type).await,
            Expr::TypedString { data_type, value } => self.analyze_typed_string(data_type, value).await,
            Expr::Substring { expr, substring_from, substring_for, } => self.analyze_substring(expr, substring_from, substring_for).await,
            Expr::Between { expr, negated, low, high } => self.analyze_between(expr, negated, low, high).await,
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
        }
    }

    async fn analyze_function(&self, function: &Function) -> Result<Expression> {
        let name = function.name.to_string();
        if AggregateFunctionFactory::instance().check(&name) {
            self.analyze_aggr_function(&name, function).await;
        }

        let mut args = Vec::with_capacity(function.args.len());

        // 1. Get the args from context by function name. such as SELECT database()
        // common::ScalarFunctions::udf::database arg is ctx.get_default()
        let ctx_args = ContextFunction::build_args_from_ctx(
            function.name.to_string().as_str(),
            self.context.clone(),
        )?;

        if !ctx_args.is_empty() {
            args.extend_from_slice(ctx_args.as_slice());
        }

        // 2. Get args from the ast::Expr:Function
        let mut args = Vec::with_capacity(function.args.len());
        for arg in &function.args {
            match &arg {
                FunctionArg::Named { arg, .. } => {
                    args.push(self.analyze(arg).await?);
                }
                FunctionArg::Unnamed(arg) => {
                    args.push(self.analyze(arg).await?);
                }
            }
        }

        Ok(Expression::ScalarFunction { op: name, args })
    }

    async fn analyze_aggr_function(&self, name: &str, function: &Function) {
        let mut arguments = Vec::with_capacity(function.args.len());
        for function_argument in &function.args {
            match &function_argument {
                FunctionArg::Named { arg, .. } => {
                    arguments.push(self.analyze(arg).await?);
                }
                FunctionArg::Unnamed(arg) => {
                    arguments.push(self.analyze(arg).await?);
                }
            }
        }

        let args = match name.to_lowercase().as_str() {
            "count" => arguments
                .iter()
                .map(|c| match c {
                    Expression::Wildcard => common_planners::lit(0i64),
                    _ => c.clone(),
                })
                .collect(),
            _ => arguments,
        };

        let params = function
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
            op: name,
            distinct: function.distinct,
            params,
            args,
        });
    }

    fn analyze_identifier(&self, ident: &Ident) -> Result<Expression> {
        let column_name = ident.clone().value;

        match self.schema.contains_column(&column_name) {
            true => Ok(Expression::Column(column_name)),
            false => Err(ErrorCode::UnknownColumn(format!("Unknown column {}. columns: {:?}", column_name, self.schema)))
        }
    }

    fn analyze_identifiers(&self, idents: &[Ident]) -> Result<Expression> {
        let mut names = Vec::with_capacity(idents.len());
        for ident in idents {
            names.push(ident.clone().value);
        }

        let schema = &self.schema;
        match schema.get_column_by_fullname(&names) {
            None => Err(ErrorCode::UnknownColumn(format!("Unknown column names {:?}", names))),
            Some(desc) => Ok(Expression::Column(desc.column_name()))
        }
    }

    async fn analyze_exists(&self, subquery: &Query) -> Result<Expression> {
        Ok(Expression::ScalarFunction {
            op: "EXISTS".to_lowercase(),
            args: vec![self.analyze_subquery(subquery).await?],
        })
    }

    pub async fn analyze_subquery(&self, subquery: &Query) -> Result<Expression> {
        let statement = DfQueryStatement::try_from(subquery.clone())?;

        let query_context = self.context.clone();
        let subquery_context = DatabendQueryContext::new(query_context);

        let analyze_subquery = statement.analyze(subquery_context);
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery.await? {
            let subquery_plan = PlanParser::build_query_plan(&analyze_data)?;
            return Ok(Expression::Subquery {
                name: query_context.get_subquery_name(&subquery_plan),
                query_plan: Arc::new(subquery_plan),
            });
        }

        Err(ErrorCode::SyntaxException(format!("Unsupported subquery type {:?}", subquery)))
    }

    pub async fn analyze_scalar_subquery(&self, subquery: &Query) -> Result<Expression> {
        let statement = DfQueryStatement::try_from(subquery.clone())?;

        let query_context = self.context.clone();
        let subquery_context = DatabendQueryContext::new(query_context);

        let analyze_subquery = statement.analyze(subquery_context);
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery.await? {
            let subquery_plan = PlanParser::build_query_plan(&analyze_data)?;
            return Ok(Expression::ScalarSubquery {
                name: query_context.get_subquery_name(&subquery_plan),
                query_plan: Arc::new(subquery_plan),
            });
        }

        Err(ErrorCode::SyntaxException(format!("Unsupported subquery type {:?}", subquery)))
    }

    async fn analyze_between(&self, expr: &Expr, negated: &bool, low: &Expr, high: &Expr) -> Result<Expression> {
        let expression = self.analyze(expr).await?;
        let low_expression = self.analyze(low).await?;
        let high_expression = self.analyze(high).await?;
        Ok(match *negated {
            false => expression
                .gt_eq(low_expression)
                .and(expression.lt(high_expression)),
            true => expression
                .lt(low_expression)
                .or(expression.gt_eq(high_expression)),
        })
    }

    async fn analyze_substring(&self, expr: &Expr, from: &Option<Box<Expr>>, length: &Option<Box<Expr>>) -> Result<Expression> {
        let mut arguments = Vec::with_capacity(3);

        arguments.push(self.analyze(expr).await?);
        if let Some(from) = from {
            arguments.push(self.analyze(from).await?);
        } else {
            arguments.push(Expression::create_literal(DataValue::Int64(Some(1))));
        }

        if let Some(len) = length {
            arguments.push(self.analyze(len).await?);
        }

        Ok(Expression::ScalarFunction {
            op: "substring".to_string(),
            args: arguments,
        })
    }

    async fn analyze_cast(&self, expr: &Expr, data_type: &DataType) -> Result<Expression> {
        let expr = self.analyze(expr).await?;
        let cast_to_type = SQLCommon::make_data_type(data_type)?;
        Ok(Expression::Cast { expr: Box::new(expr), data_type: cast_to_type })
    }

    async fn analyze_typed_string(&self, data_type: &DataType, value: &str) -> Result<Expression> {
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

    async fn analyze_binary_expr(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<Expression> {
        Ok(Expression::BinaryExpression {
            op: format!("{}", op),
            left: Box::new(self.analyze(left).await?),
            right: Box::new(self.analyze(right).await?),
        })
    }

    async fn analyze_unary_expr(&self, op: &UnaryOperator, expr: &Expr) -> Result<Expression> {
        match op {
            UnaryOperator::Plus => self.analyze(expr).await,
            _other_operator => Ok(Expression::UnaryExpression {
                op: format!("{}", op),
                expr: Box::new(self.analyze(expr).await?),
            }),
        }
    }

    async fn analyze_is_null(&self, expr: &Expr) -> Result<Expression> {
        Ok(Expression::create_scalar_function("is_null", vec![self.analyze(expr).await?]))
    }

    async fn analyze_is_not_null(&self, expr: &Expr) -> Result<Expression> {
        Ok(Expression::create_scalar_function("isnotnull", vec![self.analyze(expr).await?]))
    }
}
