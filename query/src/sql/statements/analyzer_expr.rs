// Copyright 2020 Datafuse Labs.
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

use std::convert::TryFrom;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_planners::Expression;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::UnaryOperator;
use sqlparser::ast::Value;

use crate::functions::ContextFunction;
use crate::sessions::QueryContext;
use crate::sql::statements::analyzer_value_expr::ValueExprAnalyzer;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::PlanParser;
use crate::sql::SQLCommon;

pub struct ExpressionAnalyzer {
    context: Arc<QueryContext>,
}

impl ExpressionAnalyzer {
    pub fn create(context: Arc<QueryContext>) -> ExpressionAnalyzer {
        ExpressionAnalyzer { context }
    }

    pub async fn analyze(&self, expr: &Expr) -> Result<Expression> {
        let mut stack = Vec::new();

        // Build RPN for expr. because async function unsupported recursion
        for rpn_item in &ExprRPNBuilder::build(expr)? {
            match rpn_item {
                ExprRPNItem::Value(v) => Self::analyze_value(v, &mut stack)?,
                ExprRPNItem::Identifier(v) => self.analyze_identifier(v, &mut stack)?,
                ExprRPNItem::QualifiedIdentifier(v) => self.analyze_identifiers(v, &mut stack)?,
                ExprRPNItem::Function(v) => self.analyze_function(v, &mut stack)?,
                ExprRPNItem::Wildcard => self.analyze_wildcard(&mut stack)?,
                ExprRPNItem::Exists(v) => self.analyze_exists(v, &mut stack).await?,
                ExprRPNItem::Subquery(v) => self.analyze_scalar_subquery(v, &mut stack).await?,
                ExprRPNItem::Cast(v) => self.analyze_cast(v, &mut stack)?,
                ExprRPNItem::Between(negated) => self.analyze_between(*negated, &mut stack)?,
            }
        }

        match stack.len() {
            1 => Ok(stack.remove(0)),
            _ => Err(ErrorCode::LogicalError(
                "Logical error: this is expr rpn bug.",
            )),
        }
    }

    fn analyze_value(value: &Value, args: &mut Vec<Expression>) -> Result<()> {
        args.push(ValueExprAnalyzer::analyze(value)?);
        Ok(())
    }

    fn analyze_function(&self, info: &FunctionExprInfo, args: &mut Vec<Expression>) -> Result<()> {
        let mut arguments = Vec::with_capacity(info.args_count);
        for _index in 0..info.args_count {
            match args.pop() {
                None => {
                    return Err(ErrorCode::LogicalError("It's a bug."));
                }
                Some(arg) => {
                    arguments.insert(0, arg);
                }
            }
        }

        args.push(
            match AggregateFunctionFactory::instance().check(&info.name) {
                true => self.aggr_function(info, &arguments),
                false => match (info.unary_operator, info.binary_operator) {
                    (true, _) => Self::unary_function(info, &arguments),
                    (_, true) => Self::binary_function(info, &arguments),
                    _ => self.function(info, &arguments),
                },
            }?,
        );
        Ok(())
    }

    fn unary_function(info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        match args.is_empty() {
            true => Err(ErrorCode::LogicalError(
                "Unary operator must be two children.",
            )),
            false => Ok(Expression::UnaryExpression {
                op: info.name.clone(),
                expr: Box::new(args[0].to_owned()),
            }),
        }
    }

    fn binary_function(info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        let op = info.name.clone();
        match args.len() < 2 {
            true => Err(ErrorCode::LogicalError(
                "Binary operator must be two children.",
            )),
            false => Ok(Expression::BinaryExpression {
                op,
                left: Box::new(args[0].to_owned()),
                right: Box::new(args[1].to_owned()),
            }),
        }
    }

    fn function(&self, info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        let query_context = self.context.clone();
        let context_args = ContextFunction::build_args_from_ctx(&info.name, query_context)?;

        match context_args.is_empty() {
            true => {
                let op = info.name.clone();
                let arguments = args.to_owned();
                Ok(Expression::ScalarFunction {
                    op,
                    args: arguments,
                })
            }
            false => {
                let op = info.name.clone();
                Ok(Expression::ScalarFunction {
                    op,
                    args: context_args,
                })
            }
        }
    }

    fn aggr_function(&self, info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        let mut parameters = Vec::with_capacity(info.parameters.len());

        for parameter in &info.parameters {
            match ValueExprAnalyzer::analyze(parameter)? {
                Expression::Literal { value, .. } => {
                    parameters.push(value);
                }
                expr => {
                    return Err(ErrorCode::SyntaxException(format!(
                        "Unsupported value expression: {:?}, must be datavalue",
                        expr
                    )));
                }
            };
        }

        if info.name.eq_ignore_ascii_case("count")
            && !args.is_empty()
            && matches!(args[0], Expression::Wildcard)
        {
            Ok(Expression::AggregateFunction {
                op: info.name.clone(),
                distinct: info.distinct,
                args: vec![common_planners::lit(0i64)],
                params: parameters,
            })
        } else {
            Ok(Expression::AggregateFunction {
                op: info.name.clone(),
                distinct: info.distinct,
                args: args.to_owned(),
                params: parameters,
            })
        }
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
        args.push(Expression::ScalarFunction {
            op: "EXISTS".to_lowercase(),
            args: subquery,
        });
        Ok(())
    }

    async fn analyze_subquery(&self, subquery: &Query) -> Result<Expression> {
        let statement = DfQueryStatement::try_from(subquery.clone())?;

        let query_context = self.context.clone();
        let subquery_context = QueryContext::new(query_context.clone());

        let analyze_subquery = statement.analyze(subquery_context);
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery.await? {
            let subquery_plan = PlanParser::build_query_plan(&analyze_data)?;
            return Ok(Expression::Subquery {
                name: query_context.get_subquery_name(&subquery_plan),
                query_plan: Arc::new(subquery_plan),
            });
        }

        Err(ErrorCode::SyntaxException(format!(
            "Unsupported subquery type {:?}",
            subquery
        )))
    }

    async fn analyze_scalar_subquery(
        &self,
        subquery: &Query,
        args: &mut Vec<Expression>,
    ) -> Result<()> {
        let statement = DfQueryStatement::try_from(subquery.clone())?;

        let query_context = self.context.clone();
        let subquery_context = QueryContext::new(query_context.clone());

        let analyze_subquery = statement.analyze(subquery_context);
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery.await? {
            let subquery_plan = PlanParser::build_query_plan(&analyze_data)?;
            args.push(Expression::ScalarSubquery {
                name: query_context.get_subquery_name(&subquery_plan),
                query_plan: Arc::new(subquery_plan),
            });

            return Ok(());
        }

        Err(ErrorCode::SyntaxException(format!(
            "Unsupported subquery type {:?}",
            subquery
        )))
    }

    fn analyze_wildcard(&self, arguments: &mut Vec<Expression>) -> Result<()> {
        arguments.push(Expression::Wildcard);
        Ok(())
    }

    fn analyze_cast(
        &self,
        data_type: &common_datavalues::DataType,
        args: &mut Vec<Expression>,
    ) -> Result<()> {
        match args.pop() {
            None => Err(ErrorCode::LogicalError(
                "Cast operator must be one children.",
            )),
            Some(inner_expr) => {
                args.push(Expression::Cast {
                    expr: Box::new(inner_expr),
                    data_type: data_type.clone(),
                });
                Ok(())
            }
        }
    }

    fn analyze_between(&self, negated: bool, args: &mut Vec<Expression>) -> Result<()> {
        if args.len() < 3 {
            return Err(ErrorCode::SyntaxException(
                "Between must be a ternary expression.",
            ));
        }

        let expression = args.remove(0);
        let low_expression = args.remove(0);
        let high_expression = args.remove(0);

        match negated {
            false => args.push(
                expression
                    .gt_eq(low_expression)
                    .and(expression.lt_eq(high_expression)),
            ),
            true => args.push(
                expression
                    .lt(low_expression)
                    .or(expression.gt(high_expression)),
            ),
        };

        Ok(())
    }
}

struct FunctionExprInfo {
    name: String,
    distinct: bool,
    args_count: usize,
    unary_operator: bool,
    binary_operator: bool,
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
    Between(bool),
}

impl ExprRPNItem {
    pub fn function(name: String, args_count: usize) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count,
            unary_operator: false,
            binary_operator: false,
            parameters: Vec::new(),
        })
    }

    pub fn binary_operator(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count: 2,
            unary_operator: false,
            binary_operator: true,
            parameters: Vec::new(),
        })
    }

    pub fn unary_operator(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count: 1,
            unary_operator: true,
            binary_operator: false,
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
            Expr::Nested(expr) => self.visit(expr),
            Expr::Value(value) => self.visit_value(value),
            Expr::Identifier(ident) => self.visit_identifier(ident),
            Expr::CompoundIdentifier(idents) => self.visit_identifiers(idents),
            Expr::IsNull(expr) => self.visit_simple_function(expr, "isnull"),
            Expr::IsNotNull(expr) => self.visit_simple_function(expr, "isnotnull"),
            Expr::UnaryOp { op, expr } => self.visit_unary_expr(op, expr),
            Expr::BinaryOp { left, op, right } => self.visit_binary_expr(left, op, right),
            Expr::Wildcard => self.visit_wildcard(),
            Expr::Exists(subquery) => self.visit_exists(subquery),
            Expr::Subquery(subquery) => self.visit_subquery(subquery),
            Expr::Function(function) => self.visit_function(function),
            Expr::Cast { expr, data_type } => self.visit_cast(expr, data_type),
            Expr::TypedString { data_type, value } => self.visit_typed_string(data_type, value),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => self.visit_substring(expr, substring_from, substring_for),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.visit_between(expr, negated, low, high),
            Expr::Tuple(exprs) => self.visit_tuple(exprs),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
        }
    }

    fn visit_tuple(&mut self, exprs: &[Expr]) -> Result<()> {
        match exprs.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Tuple must have at least one element.",
            )),
            1 => self.visit(&exprs[0]),
            len => {
                for expr in exprs {
                    self.visit(expr)?;
                }

                self.rpn
                    .push(ExprRPNItem::function(String::from("tuple"), len));
                Ok(())
            }
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
        self.rpn
            .push(ExprRPNItem::QualifiedIdentifier(idents.to_vec()));
        Ok(())
    }

    fn visit_exists(&mut self, subquery: &Query) -> Result<()> {
        self.rpn
            .push(ExprRPNItem::Exists(Box::new(subquery.clone())));
        Ok(())
    }

    fn visit_subquery(&mut self, subquery: &Query) -> Result<()> {
        self.rpn
            .push(ExprRPNItem::Subquery(Box::new(subquery.clone())));
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
            args_count: function.args.len(),
            unary_operator: false,
            binary_operator: false,
            parameters: function.params.to_owned(),
        }));
        Ok(())
    }

    fn visit_cast(&mut self, expr: &Expr, data_type: &DataType) -> Result<()> {
        self.visit(expr)?;
        self.rpn
            .push(ExprRPNItem::Cast(SQLCommon::make_data_type(data_type)?));
        Ok(())
    }

    fn visit_typed_string(&mut self, data_type: &DataType, value: &str) -> Result<()> {
        self.rpn.push(ExprRPNItem::Value(Value::SingleQuotedString(
            value.to_string(),
        )));
        self.rpn
            .push(ExprRPNItem::Cast(SQLCommon::make_data_type(data_type)?));
        Ok(())
    }

    fn visit_simple_function(&mut self, expr: &Expr, name: impl ToString) -> Result<()> {
        self.visit(expr)?;
        self.rpn.push(ExprRPNItem::function(name.to_string(), 1));
        Ok(())
    }

    fn visit_unary_expr(&mut self, op: &UnaryOperator, expr: &Expr) -> Result<()> {
        self.visit(expr)?;
        self.rpn.push(ExprRPNItem::unary_operator(op.to_string()));
        Ok(())
    }

    fn visit_binary_expr(&mut self, left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<()> {
        self.visit(left)?;
        self.visit(right)?;
        self.rpn.push(ExprRPNItem::binary_operator(op.to_string()));
        Ok(())
    }

    fn visit_between(
        &mut self,
        expr: &Expr,
        negated: &bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<()> {
        self.visit(expr)?;
        self.visit(low)?;
        self.visit(high)?;
        self.rpn.push(ExprRPNItem::Between(*negated));
        Ok(())
    }

    fn visit_substring(
        &mut self,
        expr: &Expr,
        from: &Option<Box<Expr>>,
        length: &Option<Box<Expr>>,
    ) -> Result<()> {
        self.visit(expr)?;

        match from {
            None => self
                .rpn
                .push(ExprRPNItem::Value(Value::Number(String::from("1"), false))),
            Some(from) => self.visit(from)?,
        };

        let name = String::from("substring");
        match length {
            None => self.rpn.push(ExprRPNItem::function(name, 2)),
            Some(length) => {
                self.visit(length)?;
                self.rpn.push(ExprRPNItem::function(name, 3));
            }
        }

        Ok(())
    }
}
