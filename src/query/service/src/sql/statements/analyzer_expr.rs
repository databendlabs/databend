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

use common_datavalues::prelude::*;
use common_datavalues::type_coercion::merge_types;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_legacy_parser::analyzer_expr_sync::*;
use common_legacy_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::ast::FunctionArgExpr;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::Value;

use crate::context_function::ContextFunction;
use crate::sessions::QueryContext;
use crate::sessions::SessionType;
use crate::sessions::TableContext;
use crate::sql::statements::analyzer_value_expr::ValueExprAnalyzer;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::PlanParser;

#[derive(Clone)]
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
        for rpn_item in &ExprRPNBuilder::build(expr)? {
            match rpn_item {
                ExprRPNItem::Value(v) => Self::analyze_value(
                    v,
                    &mut stack,
                    self.context.get_current_session().get_type(),
                )?,
                ExprRPNItem::Identifier(v) => self.analyze_identifier(v, &mut stack)?,
                ExprRPNItem::QualifiedIdentifier(v) => self.analyze_identifiers(v, &mut stack)?,
                ExprRPNItem::Function(v) => self.analyze_function(v, &mut stack)?,
                ExprRPNItem::Wildcard => self.analyze_wildcard(&mut stack)?,
                ExprRPNItem::Exists(v) => self.analyze_exists(v, &mut stack).await?,
                ExprRPNItem::Subquery(v) => self.analyze_scalar_subquery(v, &mut stack).await?,
                ExprRPNItem::Cast(v, pg_style) => self.analyze_cast(v, *pg_style, &mut stack)?,
                ExprRPNItem::Between(negated) => self.analyze_between(*negated, &mut stack)?,
                ExprRPNItem::InList(v) => self.analyze_inlist(v, &mut stack)?,
                ExprRPNItem::MapAccess(v) => self.analyze_map_access(v, &mut stack)?,
                ExprRPNItem::Array(v) => self.analyze_array(*v, &mut stack)?,
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

    fn analyze_value(value: &Value, args: &mut Vec<Expression>, typ: SessionType) -> Result<()> {
        args.push(ValueExprAnalyzer::analyze(value, typ)?);
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
        let func = match &info.over {
            Some(_) => self.window_function(info, args)?,
            None => {
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

                match AggregateFunctionFactory::instance().check(&info.name) {
                    true => self.aggr_function(info, &arguments),
                    false => match info.kind {
                        OperatorKind::Unary => Self::unary_function(info, &arguments),
                        OperatorKind::Binary => Self::binary_function(info, &arguments),
                        OperatorKind::Other => self.other_function(info, &arguments),
                    },
                }?
            }
        };

        args.push(func);
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
            match ValueExprAnalyzer::analyze(
                parameter,
                self.context.get_current_session().get_type(),
            )? {
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

        let optimize_remove_count_args = info.name.eq_ignore_ascii_case("count")
            && !info.distinct
            && (args.len() == 1 && matches!(args[0], Expression::Wildcard)
                || args.iter().all(
                    |expr| matches!(expr, Expression::Literal { value, .. } if !value.is_null()),
                ));

        if optimize_remove_count_args {
            Ok(Expression::AggregateFunction {
                op: info.name.clone(),
                distinct: info.distinct,
                args: vec![],
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

    fn window_function(
        &self,
        info: &FunctionExprInfo,
        args: &mut Vec<Expression>,
    ) -> Result<Expression> {
        // window function partition by and order by args
        let mut partition_by = vec![];
        let mut order_by_expr = vec![];
        if let Some(window_spec) = &info.over {
            let order_by_args_count = window_spec.order_by.len();
            let partition_by_args_count = window_spec.partition_by.len();

            for i in 0..partition_by_args_count + order_by_args_count {
                match args.pop() {
                    None => {
                        return Err(ErrorCode::LogicalError("It's a bug."));
                    }
                    Some(arg) => {
                        if i < order_by_args_count {
                            order_by_expr.insert(0, arg);
                        } else {
                            partition_by.insert(0, arg);
                        }
                    }
                }
            }
        }

        let mut parameters = Vec::with_capacity(info.parameters.len());

        for parameter in &info.parameters {
            match ValueExprAnalyzer::analyze(
                parameter,
                self.context.get_current_session().get_type(),
            )? {
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

        let window_spec = info.over.as_ref().unwrap();

        let order_by: Vec<Expression> = order_by_expr
            .into_iter()
            .zip(window_spec.order_by.clone())
            .map(|(expr_sort_on, parser_sort_expr)| Expression::Sort {
                expr: Box::new(expr_sort_on.clone()),
                asc: parser_sort_expr.asc.unwrap_or(true),
                nulls_first: parser_sort_expr.nulls_first.unwrap_or(true),
                origin_expr: Box::new(expr_sort_on),
            })
            .collect();

        let window_frame = window_spec
            .window_frame
            .clone()
            .map(|wf| wf.try_into().unwrap());

        Ok(Expression::WindowFunction {
            op: info.name.clone(),
            params: parameters,
            args: arguments,
            partition_by,
            order_by,
            window_frame,
        })
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

        let analyze_subquery = statement.analyze(subquery_context).await;
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery? {
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

        let analyze_subquery = statement.analyze(subquery_context).await?;
        if let AnalyzedResult::SelectQuery(analyze_data) = analyze_subquery {
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
        data_type: &DataTypeImpl,
        pg_style: bool,
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
                    pg_style,
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

    fn analyze_map_access(&self, keys: &[Value], args: &mut Vec<Expression>) -> Result<()> {
        match args.pop() {
            None => Err(ErrorCode::LogicalError(
                "MapAccess operator must be one children.",
            )),
            Some(inner_expr) => {
                let name = match &keys[0] {
                    k @ Value::Number(_, _) => format!("{}[{}]", inner_expr.column_name(), k),
                    Value::SingleQuotedString(s) => {
                        format!("{}['{}']", inner_expr.column_name(), s)
                    }
                    Value::DoubleQuotedString(s) => {
                        format!("{}[\"{}\"]", inner_expr.column_name(), s)
                    }
                    Value::ColonString(s) => format!("{}:{}", inner_expr.column_name(), s),
                    Value::PeriodString(s) => format!("{}.{}", inner_expr.column_name(), s),
                    _ => format!("{}[{}]", inner_expr.column_name(), keys[0]),
                };

                let path_expr = match &keys[0] {
                    Value::Number(value, _) => Expression::create_literal(
                        DataValue::try_from_literal(value, None).unwrap(),
                    ),
                    Value::SingleQuotedString(s)
                    | Value::DoubleQuotedString(s)
                    | Value::ColonString(s)
                    | Value::PeriodString(s) => Expression::create_literal(s.as_bytes().into()),
                    _ => Expression::create_literal(keys[0].to_string().as_bytes().into()),
                };
                let arguments = vec![inner_expr, path_expr];

                args.push(Expression::MapAccess {
                    name,
                    args: arguments,
                });
                // convert map access v[0][1] to function get(get(v, 0), 1)
                if keys.len() > 1 {
                    self.analyze_map_access(&keys[1..], args)?;
                }
                Ok(())
            }
        }
    }

    fn analyze_array(&self, nums: usize, args: &mut Vec<Expression>) -> Result<()> {
        let mut values = Vec::with_capacity(nums);
        let mut types = Vec::with_capacity(nums);
        for _ in 0..nums {
            match args.pop() {
                None => {
                    break;
                }
                Some(inner_expr) => {
                    if let Expression::Literal {
                        value, data_type, ..
                    } = inner_expr
                    {
                        values.push(value);
                        types.push(data_type);
                    }
                }
            };
        }
        if values.len() != nums {
            return Err(ErrorCode::LogicalError(format!(
                "Array must have {} children.",
                nums
            )));
        }
        let inner_type = if types.is_empty() {
            NullType::new_impl()
        } else {
            types
                .iter()
                .fold(Ok(types[0].clone()), |acc, v| merge_types(&acc?, v))
                .map_err(|e| ErrorCode::LogicalError(e.message()))?
        };
        values.reverse();

        let array_value = Expression::create_literal_with_type(
            DataValue::Array(values),
            ArrayType::new_impl(inner_type),
        );
        args.push(array_value);
        Ok(())
    }
}

use common_legacy_parser::SQLDialect;
impl From<SessionType> for SQLDialect {
    fn from(session_type: SessionType) -> Self {
        match session_type {
            SessionType::MySQL => SQLDialect::MySQL,
            _ => SQLDialect::Other,
        }
    }
}
