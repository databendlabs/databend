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

use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use common_ast::parser::expr::ExprTraverser;
use common_ast::parser::expr::ExprVisitor;
use common_ast::udfs::UDFDefinition;
use common_ast::udfs::UDFFetcher;
use common_ast::udfs::UDFParser;
use common_ast::udfs::UDFTransformer;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::is_builtin_function;
use common_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::ast::FunctionArgExpr;
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

        // Build RPN for expr. Because async function unsupported recursion
        for rpn_item in &ExprRPNBuilder::build(self.context.clone(), expr).await? {
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
                ExprRPNItem::InList(v) => self.analyze_inlist(v, &mut stack)?,
            }
        }

        match stack.len() {
            1 => Ok(stack.remove(0)),
            _ => Err(ErrorCode::LogicalError(
                "Logical error: this is expr rpn bug.",
            )),
        }
    }

    pub async fn analyze_function_arg(&self, arg_expr: &FunctionArgExpr) -> Result<Expression> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => self.analyze(expr).await,
            FunctionArgExpr::Wildcard => Ok(Expression::Wildcard),
            FunctionArgExpr::QualifiedWildcard(_) => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported arg statement: {}",
                arg_expr
            ))),
        }
    }

    fn analyze_value(value: &Value, args: &mut Vec<Expression>) -> Result<()> {
        args.push(ValueExprAnalyzer::analyze(value)?);
        Ok(())
    }

    fn analyze_inlist(&self, info: &InListInfo, args: &mut Vec<Expression>) -> Result<()> {
        let mut list = Vec::with_capacity(info.list_size);
        for _ in 0..info.list_size {
            match args.pop() {
                None => {
                    return Err(ErrorCode::LogicalError("It's a bug."));
                }
                Some(arg) => {
                    list.insert(0, arg);
                }
            }
        }

        let expr = args
            .pop()
            .ok_or_else(|| ErrorCode::LogicalError("It's a bug."))?;
        list.insert(0, expr);

        let op = if info.negated {
            "NOT_IN".to_string()
        } else {
            "IN".to_string()
        };

        args.push(Expression::ScalarFunction { op, args: list });
        Ok(())
    }

    fn analyze_function(&self, info: &FunctionExprInfo, args: &mut Vec<Expression>) -> Result<()> {
        let mut arguments = Vec::with_capacity(info.args_count);
        for _ in 0..info.args_count {
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
                false => match info.kind {
                    OperatorKind::Unary => Self::unary_function(info, &arguments),
                    OperatorKind::Binary => Self::binary_function(info, &arguments),
                    OperatorKind::Other => self.other_function(info, &arguments),
                },
            }?,
        );
        Ok(())
    }

    fn unary_function(info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        match args.is_empty() {
            true => Err(ErrorCode::LogicalError("Unary operator must be one child.")),
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

    /// Function to process when args's size is more than 2.
    fn other_function(&self, info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        let query_context = self.context.clone();
        let context_args = ContextFunction::build_args_from_ctx(query_context, &info.name)?;

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
        let subquery_context = QueryContext::create_from(query_context.clone());

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
        let subquery_context = QueryContext::create_from(query_context.clone());

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

    fn analyze_cast(&self, data_type: &DataTypePtr, args: &mut Vec<Expression>) -> Result<()> {
        match args.pop() {
            None => Err(ErrorCode::LogicalError(
                "Cast operator must be one children.",
            )),
            Some(inner_expr) => {
                args.push(Expression::Cast {
                    expr: Box::new(inner_expr),
                    data_type: data_type.clone(),
                    is_nullable: false,
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

        let s_args = args.split_off(args.len() - 3);
        let expression = s_args[0].clone();
        let low_expression = s_args[1].clone();
        let high_expression = s_args[2].clone();

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

enum OperatorKind {
    Unary,
    Binary,
    Other,
}

struct FunctionExprInfo {
    name: String,
    distinct: bool,
    args_count: usize,
    kind: OperatorKind,
    parameters: Vec<Value>,
}

struct InListInfo {
    list_size: usize,
    negated: bool,
}

enum ExprRPNItem {
    Value(Value),
    Identifier(Ident),
    QualifiedIdentifier(Vec<Ident>),
    Function(FunctionExprInfo),
    Wildcard,
    Exists(Box<Query>),
    Subquery(Box<Query>),
    Cast(DataTypePtr),
    Between(bool),
    InList(InListInfo),
}

impl ExprRPNItem {
    pub fn function(name: String, args_count: usize) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count,
            kind: OperatorKind::Other,
            parameters: Vec::new(),
        })
    }

    pub fn binary_operator(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count: 2,
            kind: OperatorKind::Binary,
            parameters: Vec::new(),
        })
    }

    pub fn unary_operator(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count: 1,
            kind: OperatorKind::Unary,
            parameters: Vec::new(),
        })
    }
}

struct ExprRPNBuilder {
    rpn: Vec<ExprRPNItem>,
    context: Arc<QueryContext>,
}

impl ExprRPNBuilder {
    pub async fn build(context: Arc<QueryContext>, expr: &Expr) -> Result<Vec<ExprRPNItem>> {
        let mut builder = ExprRPNBuilder {
            context,
            rpn: Vec::new(),
        };
        ExprTraverser::accept(expr, &mut builder).await?;
        Ok(builder.rpn)
    }

    fn process_expr(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Value(value) => {
                self.rpn.push(ExprRPNItem::Value(value.clone()));
            }
            Expr::Identifier(ident) => {
                self.rpn.push(ExprRPNItem::Identifier(ident.clone()));
            }
            Expr::CompoundIdentifier(idents) => {
                self.rpn
                    .push(ExprRPNItem::QualifiedIdentifier(idents.to_vec()));
            }
            Expr::IsNull(_) => {
                self.rpn
                    .push(ExprRPNItem::function(String::from("isnull"), 1));
            }
            Expr::IsNotNull(_) => {
                self.rpn
                    .push(ExprRPNItem::function(String::from("isnotnull"), 1));
            }
            Expr::UnaryOp { op, .. } => {
                match op {
                    UnaryOperator::Plus => {}
                    // In order to distinguish it from binary addition.
                    UnaryOperator::Minus => self
                        .rpn
                        .push(ExprRPNItem::unary_operator("NEGATE".to_string())),
                    _ => self.rpn.push(ExprRPNItem::unary_operator(op.to_string())),
                }
            }
            Expr::BinaryOp { op, .. } => {
                self.rpn.push(ExprRPNItem::binary_operator(op.to_string()));
            }
            Expr::Exists(subquery) => {
                self.rpn.push(ExprRPNItem::Exists(subquery.clone()));
            }
            Expr::Subquery(subquery) => {
                self.rpn.push(ExprRPNItem::Subquery(subquery.clone()));
            }
            Expr::Function(function) => {
                self.rpn.push(ExprRPNItem::Function(FunctionExprInfo {
                    name: function.name.to_string(),
                    distinct: function.distinct,
                    args_count: function.args.len(),
                    kind: OperatorKind::Other,
                    parameters: function.params.to_owned(),
                }));
            }
            Expr::Cast { data_type, .. } => {
                self.rpn
                    .push(ExprRPNItem::Cast(SQLCommon::make_data_type(data_type)?));
            }
            Expr::TypedString { data_type, value } => {
                self.rpn.push(ExprRPNItem::Value(Value::SingleQuotedString(
                    value.to_string(),
                )));
                self.rpn
                    .push(ExprRPNItem::Cast(SQLCommon::make_data_type(data_type)?));
            }
            Expr::Position { .. } => {
                let name = String::from("position");
                self.rpn.push(ExprRPNItem::function(name, 2));
            }
            Expr::Substring {
                substring_from,
                substring_for,
                ..
            } => {
                if substring_from.is_none() {
                    self.rpn
                        .push(ExprRPNItem::Value(Value::Number(String::from("1"), false)));
                }

                let name = String::from("substring");
                match substring_for {
                    None => self.rpn.push(ExprRPNItem::function(name, 2)),
                    Some(_) => {
                        self.rpn.push(ExprRPNItem::function(name, 3));
                    }
                }
            }
            Expr::Between { negated, .. } => {
                self.rpn.push(ExprRPNItem::Between(*negated));
            }
            Expr::Tuple(exprs) => {
                let len = exprs.len();

                if len > 1 {
                    self.rpn
                        .push(ExprRPNItem::function(String::from("tuple"), len));
                }
            }
            Expr::InList {
                expr: _,
                list,
                negated,
            } => self.rpn.push(ExprRPNItem::InList(InListInfo {
                list_size: list.len(),
                negated: *negated,
            })),
            _ => (),
        }

        Ok(())
    }
}

#[async_trait]
impl UDFFetcher for ExprRPNBuilder {
    async fn get_udf_definition(&self, name: &str) -> Result<UDFDefinition> {
        let tenant = self.context.get_tenant();
        let udf = self
            .context
            .get_user_manager()
            .get_udf(&tenant, name)
            .await?;
        let mut udf_parser = UDFParser::default();
        let definition = udf_parser
            .parse(&udf.name, &udf.parameters, &udf.definition)
            .await?;

        Ok(UDFDefinition::new(udf.parameters, definition))
    }
}

#[async_trait]
impl ExprVisitor for ExprRPNBuilder {
    async fn pre_visit(&mut self, expr: &Expr) -> Result<Expr> {
        if let Expr::Function(function) = expr {
            if !is_builtin_function(&function.name.to_string()) {
                return UDFTransformer::transform_function(function, self).await;
            }
        }

        Ok(expr.clone())
    }

    async fn post_visit(&mut self, expr: &Expr) -> Result<()> {
        self.process_expr(expr)
    }

    fn visit_wildcard(&mut self) -> Result<()> {
        self.rpn.push(ExprRPNItem::Wildcard);
        Ok(())
    }
}
