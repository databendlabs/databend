use std::convert::TryFrom;
use std::sync::Arc;

use sqlparser::ast::{BinaryOperator, DataType, Expr, Function, FunctionArg, Ident, Query, Value};

use common_exception::{ErrorCode, Result};
use common_functions::aggregates::AggregateFunctionFactory;
use common_planners::Expression;

use crate::sessions::{DatabendQueryContext, DatabendQueryContextRef};
use crate::sql::{PlanParser, SQLCommon};
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult, DfQueryStatement};
use crate::sql::statements::analyzer_value_expr::ValueExprAnalyzer;
use crate::sql::statements::query::JoinedSchema;

pub struct ExpressionAnalyzer {
    context: DatabendQueryContextRef,
}

impl ExpressionAnalyzer {
    pub fn create(ctx: DatabendQueryContextRef) -> ExpressionAnalyzer {
        ExpressionAnalyzer { context: ctx.clone() }
    }

    pub async fn analyze(&self, expr: &Expr) -> Result<Expression> {
        let mut arguments = Vec::new();

        // Build RPN for expr. because async function unsupported recursion
        for rpn_item in &ExprRPNBuilder::build(expr)? {
            match rpn_item {
                ExprRPNItem::Value(value) => Self::analyze_value(value, &mut arguments)?,
                ExprRPNItem::Identifier(ident) => self.analyze_identifier(ident, &mut arguments)?,
                ExprRPNItem::QualifiedIdentifier(idents) => self.analyze_identifiers(idents, &mut arguments)?,
                ExprRPNItem::Function(info) => self.analyze_function(info, &mut arguments)?,
                ExprRPNItem::Wildcard => self.analyze_wildcard(&mut arguments)?,
                ExprRPNItem::Exists(subquery) => self.analyze_exists(subquery, &mut arguments).await?,
                ExprRPNItem::Subquery(subquery) => self.analyze_scalar_subquery(subquery, &mut arguments).await?,
                ExprRPNItem::Cast(data_type) => self.analyze_cast(data_type, &mut arguments)?,
            }
        }

        match arguments.len() {
            1 => Ok(arguments.remove(0)),
            _ => Err(ErrorCode::LogicalError("Logical error: this is expr rpn bug.")),
        }
    }

    fn analyze_value(value: &Value, args: &mut Vec<Expression>) -> Result<()> {
        args.push(ValueExprAnalyzer::analyze(value)?);
        Ok(())
    }

    fn analyze_function(&self, info: &FunctionExprInfo, arguments: &mut Vec<Expression>) -> Result<()> {
        match AggregateFunctionFactory::instance().check(&info.name) {
            true => self.analyze_aggr_function(info, arguments),
            false => {
                let op = info.name.clone();
                let args = arguments.to_owned();

                arguments.clear();
                arguments.push(Expression::ScalarFunction { op, args });
                Ok(())
            }
        }
    }

    fn analyze_aggr_function(&self, info: &FunctionExprInfo, args: &mut Vec<Expression>) -> Result<()> {
        let mut arguments = Vec::with_capacity(args.len());
        let mut parameters = Vec::with_capacity(info.parameters.len());

        while !args.is_empty() {
            match args.remove(0) {
                Expression::Wildcard if info.name.eq_ignore_ascii_case("count") => {
                    arguments.push(common_planners::lit(0i64));
                }
                argument => { arguments.push(argument); }
            };
        }

        for parameter in &info.parameters {
            match ValueExprAnalyzer::analyze(parameter)? {
                Expression::Literal { value, .. } => { parameters.push(value); }
                expr => {
                    return Err(ErrorCode::SyntaxException(format!(
                        "Unsupported value expression: {:?}, must be datavalue",
                        expr
                    )));
                }
            };
        }

        args.push(Expression::AggregateFunction {
            op: info.name.clone(),
            distinct: info.distinct,
            args: arguments,
            params: parameters,
        });

        Ok(())
    }

    fn analyze_identifier(&self, ident: &Ident, arguments: &mut Vec<Expression>) -> Result<()> {
        let column_name = ident.clone().value;
        arguments.push(Expression::Column(column_name));
        Ok(())
    }

    fn analyze_identifiers(&self, idents: &[Ident], arguments: &mut Vec<Expression>) -> Result<()> {
        let mut names = Vec::with_capacity(idents.len());

        for ident in idents {
            names.push(ident.clone().value);
        }

        arguments.push(Expression::QualifiedColumn(names));
        Ok(())
    }

    async fn analyze_exists(&self, subquery: &Query, args: &mut Vec<Expression>) -> Result<()> {
        let subquery = vec![self.analyze_subquery(subquery).await?];
        args.push(Expression::ScalarFunction { op: "EXISTS".to_lowercase(), args: subquery });
        Ok(())
    }

    async fn analyze_subquery(&self, subquery: &Query) -> Result<Expression> {
        let statement = DfQueryStatement::try_from(subquery.clone())?;

        let query_context = self.context.clone();
        let subquery_context = DatabendQueryContext::new(query_context.clone());

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

    async fn analyze_scalar_subquery(&self, subquery: &Query, args: &mut Vec<Expression>) -> Result<()> {
        let statement = DfQueryStatement::try_from(subquery.clone())?;

        let query_context = self.context.clone();
        let subquery_context = DatabendQueryContext::new(query_context.clone());

        let analyze_subquery = statement.analyze(subquery_context);
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery.await? {
            let subquery_plan = PlanParser::build_query_plan(&analyze_data)?;
            args.push(Expression::ScalarSubquery {
                name: query_context.get_subquery_name(&subquery_plan),
                query_plan: Arc::new(subquery_plan),
            });

            return Ok(());
        }

        Err(ErrorCode::SyntaxException(format!("Unsupported subquery type {:?}", subquery)))
    }

    fn analyze_wildcard(&self, arguments: &mut Vec<Expression>) -> Result<()> {
        arguments.push(Expression::Wildcard);
        Ok(())
    }

    fn analyze_cast(&self, data_type: &common_datavalues::DataType, args: &mut Vec<Expression>) -> Result<()> {
        let expr = args.remove(0);
        args.push(Expression::Cast { expr: Box::new(expr), data_type: data_type.clone() });
        Ok(())
    }
}

struct FunctionExprInfo {
    name: String,
    distinct: bool,
    parameters: Vec<Value>,
}

enum ExprRPNItem {
    Value(Value),
    Identifier(Ident),
    QualifiedIdentifier(Vec<Ident>),
    Function(FunctionExprInfo),
    Wildcard,
    Exists(Box<Query>),
    Subquery(Box<Query>),
    Cast(common_datavalues::DataType),
}

impl ExprRPNItem {
    pub fn function(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            parameters: Vec::new(),
        })
    }
}

struct ExprRPNBuilder {
    rpn: Vec<ExprRPNItem>,
}

impl ExprRPNBuilder {
    pub fn build(expr: &Expr) -> Result<Vec<ExprRPNItem>> {
        let mut builder = ExprRPNBuilder { rpn: Vec::new() };
        builder.visit(expr)?;
        Ok(builder.rpn)
    }

    fn visit(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Nested(expr) => self.visit(&expr),
            Expr::Value(value) => self.visit_value(value),
            Expr::Identifier(ident) => self.visit_identifier(ident),
            Expr::CompoundIdentifier(idents) => self.visit_identifiers(idents),
            Expr::IsNull(expr) => self.visit_simple_function(&expr, "isnull"),
            Expr::IsNotNull(expr) => self.visit_simple_function(&expr, "isnotnull"),
            Expr::UnaryOp { op, expr } => self.visit_simple_function(&expr, op.to_string()),
            Expr::BinaryOp { left, op, right } => self.visit_binary_expr(left, op, right),
            Expr::Wildcard => self.visit_wildcard(),
            Expr::Exists(subquery) => self.visit_exists(subquery),
            Expr::Subquery(subquery) => self.visit_subquery(subquery),
            Expr::Function(function) => self.visit_function(function),
            Expr::Cast { expr, data_type } => self.visit_cast(expr, data_type),
            Expr::TypedString { data_type, value } => self.visit_typed_string(data_type, value),
            Expr::Substring { expr, substring_from, substring_for, } => self.visit_substring(expr, substring_from, substring_for),
            Expr::Between { expr, negated, low, high } => self.visit_between(expr, negated, low, high),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
        }
    }

    fn visit_wildcard(&mut self) -> Result<()> {
        self.rpn.push(ExprRPNItem::Wildcard);
        Ok(())
    }

    fn visit_value(&mut self, value: &Value) -> Result<()> {
        self.rpn.push(ExprRPNItem::Value(value.clone()));
        Ok(())
    }

    fn visit_identifier(&mut self, ident: &Ident) -> Result<()> {
        self.rpn.push(ExprRPNItem::Identifier(ident.clone()));
        Ok(())
    }

    fn visit_identifiers(&mut self, idents: &[Ident]) -> Result<()> {
        self.rpn.push(ExprRPNItem::QualifiedIdentifier(idents.to_vec()));
        Ok(())
    }

    fn visit_exists(&mut self, subquery: &Query) -> Result<()> {
        self.rpn.push(ExprRPNItem::Exists(Box::new(subquery.clone())));
        Ok(())
    }

    fn visit_subquery(&mut self, subquery: &Query) -> Result<()> {
        self.rpn.push(ExprRPNItem::Subquery(Box::new(subquery.clone())));
        Ok(())
    }

    fn visit_function(&mut self, function: &Function) -> Result<()> {
        // TODO: context function.
        for function_arg in &function.args {
            match function_arg {
                FunctionArg::Named { arg, .. } => self.visit(arg)?,
                FunctionArg::Unnamed(expr) => self.visit(expr)?,
            };
        }

        self.rpn.push(ExprRPNItem::Function(FunctionExprInfo {
            name: function.name.to_string(),
            distinct: function.distinct,
            parameters: function.params.to_owned(),
        }));
        Ok(())
    }

    fn visit_cast(&mut self, expr: &Expr, data_type: &DataType) -> Result<()> {
        self.visit(expr)?;
        self.rpn.push(ExprRPNItem::Cast(SQLCommon::make_data_type(data_type)?));
        Ok(())
    }

    fn visit_typed_string(&mut self, data_type: &DataType, value: &str) -> Result<()> {
        self.rpn.push(ExprRPNItem::Value(Value::SingleQuotedString(value.to_string())));
        self.rpn.push(ExprRPNItem::Cast(SQLCommon::make_data_type(data_type)?));
        Ok(())
    }

    fn visit_simple_function(&mut self, expr: &Expr, name: impl ToString) -> Result<()> {
        self.visit(expr)?;
        self.rpn.push(ExprRPNItem::function(name.to_string()));
        Ok(())
    }

    fn visit_binary_expr(&mut self, left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<()> {
        self.visit(left)?;
        self.visit(right)?;
        self.rpn.push(ExprRPNItem::function(op.to_string()));
        Ok(())
    }

    fn visit_between(&mut self, expr: &Expr, negated: &bool, low: &Expr, high: &Expr) -> Result<()> {
        self.visit(expr)?;
        self.visit(low)?;

        match *negated {
            true => {
                self.rpn.push(ExprRPNItem::function(String::from(">=")));
                self.visit(high)?;
                self.rpn.push(ExprRPNItem::function(String::from("<")));
            }
            false => {
                self.rpn.push(ExprRPNItem::function(String::from("<")));
                self.visit(high)?;
                self.rpn.push(ExprRPNItem::function(String::from(">=")));
            }
        }

        Ok(())
    }

    fn visit_substring(&mut self, expr: &Expr, from: &Option<Box<Expr>>, length: &Option<Box<Expr>>) -> Result<()> {
        self.visit(expr)?;

        // TODO: default from argument
        if let Some(expr) = from {
            self.visit(&expr)?;
        }

        if let Some(expr) = length {
            self.visit(&expr)?;
        }

        Ok(())
    }
}
