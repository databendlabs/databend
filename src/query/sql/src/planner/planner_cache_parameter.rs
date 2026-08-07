// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as AstFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitorMut as AstVisitorMut;
use databend_common_ast::visit::WalkMut;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::PLAN_PARAMETER_FUNCTION;
use parking_lot::RwLock;

use crate::optimizer::ir::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::Operator;
use crate::plans::Plan;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::VisitorMut;

#[derive(Clone, Debug)]
pub(crate) struct ParameterizedStatement {
    pub(crate) template: Statement,
    pub(crate) values: Vec<Scalar>,
    normalized_sql: String,
}

impl ParameterizedStatement {
    pub(crate) fn create(stmt: &Statement) -> Result<Self> {
        let mut template = stmt.clone();
        let mut marker_visitor = ExistingMarkerVisitor::default();
        template.walk_mut(&mut marker_visitor)?;
        // SQL text must not supply markers that template instantiation will consume.
        if marker_visitor.found {
            return Ok(Self {
                template: stmt.clone(),
                values: Vec::new(),
                normalized_sql: stmt.to_string(),
            });
        }

        let mut visitor = ParameterizeVisitor::default();
        template.walk_mut(&mut visitor)?;

        let mut normalized = template.clone();
        normalized.walk_mut(&mut NormalizeParameters)?;
        Ok(Self {
            template,
            values: visitor.values,
            normalized_sql: normalized.to_string(),
        })
    }

    pub(crate) fn is_parameterized(&self) -> bool {
        !self.values.is_empty()
    }

    pub(crate) fn cache_key_sql(&self) -> &str {
        &self.normalized_sql
    }
}

fn is_plan_parameter(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::FunctionCall { func, .. }
            if func.name.name.eq_ignore_ascii_case(PLAN_PARAMETER_FUNCTION)
    )
}

#[derive(Default)]
struct ExistingMarkerVisitor {
    found: bool,
}

impl AstVisitorMut for ExistingMarkerVisitor {
    type Error = ErrorCode;

    fn visit_expr(&mut self, expr: &mut Expr) -> Result<VisitControl> {
        if is_plan_parameter(expr) {
            self.found = true;
            return Ok(VisitControl::SkipChildren);
        }

        Ok(VisitControl::Continue)
    }
}

#[derive(Default)]
struct ParameterizeVisitor {
    values: Vec<Scalar>,
}

impl ParameterizeVisitor {
    fn parameterize_value_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::Literal { .. } => self.parameterize_literal(expr),
            Expr::UnaryOp {
                op: UnaryOperator::Plus | UnaryOperator::Minus,
                expr,
                ..
            } => self.parameterize_value_expr(expr),
            _ => {}
        }
    }

    fn parameterize_literal(&mut self, expr: &mut Expr) {
        let Expr::Literal { span, value } = expr else {
            return;
        };
        if matches!(value, Literal::Null) {
            return;
        }

        let span = *span;
        let value = value.clone();
        let index = self.values.len();
        self.values.push(literal_to_scalar(&value));

        *expr = Expr::FunctionCall {
            span,
            func: AstFunctionCall {
                name: Identifier::from_name(span, PLAN_PARAMETER_FUNCTION),
                args: vec![
                    Expr::Literal {
                        span,
                        value: Literal::UInt64(index as u64),
                    },
                    Expr::Literal { span, value },
                ],
                ..Default::default()
            },
        };
    }
}

impl AstVisitorMut for ParameterizeVisitor {
    type Error = ErrorCode;

    fn visit_expr(&mut self, expr: &mut Expr) -> Result<VisitControl> {
        if is_plan_parameter(expr) {
            return Ok(VisitControl::SkipChildren);
        }

        match expr {
            Expr::IsDistinctFrom { left, right, .. } => {
                self.parameterize_value_expr(left);
                self.parameterize_value_expr(right);
            }
            Expr::InList { list, .. } => {
                for item in list {
                    self.parameterize_value_expr(item);
                }
            }
            Expr::Between { low, high, .. } => {
                self.parameterize_value_expr(low);
                self.parameterize_value_expr(high);
            }
            Expr::BinaryOp {
                op, left, right, ..
            } if is_parameterized_binary_operator(op) => {
                self.parameterize_value_expr(left);
                self.parameterize_value_expr(right);
            }
            Expr::LikeAnyWithEscape { right, .. } | Expr::LikeWithEscape { right, .. } => {
                self.parameterize_value_expr(right);
            }
            _ => {}
        }

        Ok(VisitControl::Continue)
    }
}

struct NormalizeParameters;

impl AstVisitorMut for NormalizeParameters {
    type Error = ErrorCode;

    fn visit_expr(&mut self, expr: &mut Expr) -> Result<VisitControl> {
        let Expr::FunctionCall { func, .. } = expr else {
            return Ok(VisitControl::Continue);
        };
        if !func.name.name.eq_ignore_ascii_case(PLAN_PARAMETER_FUNCTION) {
            return Ok(VisitControl::Continue);
        }

        let Some(Expr::Literal { value, .. }) = func.args.get_mut(1) else {
            return Err(ErrorCode::Internal(
                "invalid planner-cache parameter marker".to_string(),
            ));
        };
        *value = canonical_literal(value);
        Ok(VisitControl::SkipChildren)
    }
}

fn is_parameterized_binary_operator(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::Gte
            | BinaryOperator::Lte
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Like(_)
            | BinaryOperator::NotLike(_)
            | BinaryOperator::LikeAny(_)
            | BinaryOperator::ILike(_)
            | BinaryOperator::NotILike(_)
            | BinaryOperator::ILikeAny(_)
            | BinaryOperator::Regexp
            | BinaryOperator::PgRegexpMatch
            | BinaryOperator::RLike
            | BinaryOperator::NotRegexp
            | BinaryOperator::NotRLike
            | BinaryOperator::SoundsLike
    )
}

fn canonical_literal(value: &Literal) -> Literal {
    match value {
        Literal::UInt64(_) => Literal::UInt64(0),
        Literal::Decimal256 {
            precision, scale, ..
        } => Literal::Decimal256 {
            value: 0.into(),
            precision: *precision,
            scale: *scale,
        },
        Literal::Float64(_) => Literal::Float64(0.0),
        Literal::String(_) => Literal::String(String::new()),
        Literal::Binary(_) => Literal::Binary(Vec::new()),
        Literal::Boolean(_) => Literal::Boolean(false),
        Literal::Null => Literal::Null,
    }
}

fn literal_to_scalar(value: &Literal) -> Scalar {
    match value {
        Literal::UInt64(value) => Scalar::Number(NumberScalar::UInt64(*value)),
        Literal::Decimal256 {
            value,
            precision,
            scale,
        } => Scalar::Decimal(DecimalScalar::Decimal256(
            i256(*value),
            DecimalSize::new_unchecked(*precision, *scale),
        )),
        Literal::Float64(value) => Scalar::Number(NumberScalar::Float64((*value).into())),
        Literal::String(value) => Scalar::String(value.clone()),
        Literal::Binary(value) => Scalar::Binary(value.clone()),
        Literal::Boolean(value) => Scalar::Boolean(*value),
        Literal::Null => Scalar::Null,
    }
}

pub(crate) fn instantiate_plan(
    template: &Plan,
    values: &[Scalar],
    formatted_ast: Option<String>,
) -> Result<Plan> {
    let Plan::Query {
        s_expr,
        metadata,
        bind_context,
        rewrite_kind,
        formatted_ast: cached_formatted_ast,
        ignore_result,
        ..
    } = template
    else {
        return Err(ErrorCode::Internal(
            "parameterized planner cache contains a non-query plan".to_string(),
        ));
    };

    let mut instantiator = ParameterInstantiator::new(values);
    let s_expr = instantiate_s_expr(s_expr, &mut instantiator)?;

    let mut metadata = metadata.read().clone();
    let mut agg_indices = metadata.agg_indices().clone();
    for indices in agg_indices.values_mut() {
        for (_, _, expr) in indices {
            *expr = instantiate_s_expr(expr, &mut instantiator)?;
        }
    }
    metadata.replace_agg_indices(agg_indices);
    instantiator.finish()?;

    Ok(Plan::Query {
        s_expr: Box::new(s_expr),
        metadata: Arc::new(RwLock::new(metadata)),
        bind_context: bind_context.clone(),
        rewrite_kind: rewrite_kind.clone(),
        formatted_ast: cached_formatted_ast
            .as_ref()
            .map(|_| formatted_ast.expect("formatted AST is present when result cache is enabled")),
        ignore_result: *ignore_result,
    })
}

fn instantiate_s_expr(
    template: &SExpr,
    instantiator: &mut ParameterInstantiator<'_>,
) -> Result<SExpr> {
    let mut plan = template.plan().clone();
    let mut result = Ok(());
    plan.visit_scalar_expr_mut(&mut |expr| {
        if result.is_ok() {
            result = instantiator.visit(expr);
        }
    });
    result?;

    let children = template
        .children()
        .map(|child| instantiate_s_expr(child, instantiator).map(Arc::new))
        .collect::<Result<Vec<_>>>()?;

    Ok(SExpr::create(plan, children, None, None, None))
}

struct ParameterInstantiator<'a> {
    values: &'a [Scalar],
    seen: Vec<bool>,
}

impl<'a> ParameterInstantiator<'a> {
    fn new(values: &'a [Scalar]) -> Self {
        Self {
            values,
            seen: vec![false; values.len()],
        }
    }

    fn finish(self) -> Result<()> {
        if self.seen.iter().all(|seen| *seen) {
            Ok(())
        } else {
            Err(ErrorCode::Internal(
                "parameterized planner cache template did not consume all parameters".to_string(),
            ))
        }
    }

    fn marker_index(function: &FunctionCall) -> Result<usize> {
        let Some(ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::Number(index),
            ..
        })) = function.arguments.first()
        else {
            return Err(ErrorCode::Internal(
                "invalid parameter index in planner cache template".to_string(),
            ));
        };

        let index = match index {
            NumberScalar::UInt8(value) => *value as usize,
            NumberScalar::UInt16(value) => *value as usize,
            NumberScalar::UInt32(value) => *value as usize,
            NumberScalar::UInt64(value) => *value as usize,
            _ => {
                return Err(ErrorCode::Internal(
                    "non-unsigned parameter index in planner cache template".to_string(),
                ));
            }
        };
        Ok(index)
    }
}

impl<'a, 'b> VisitorMut<'a> for ParameterInstantiator<'b> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::FunctionCall(function) = expr
            && function
                .func_name
                .eq_ignore_ascii_case(PLAN_PARAMETER_FUNCTION)
        {
            let index = Self::marker_index(function)?;
            let Some(value) = self.values.get(index) else {
                return Err(ErrorCode::Internal(format!(
                    "parameter index {index} is out of bounds for planner cache template"
                )));
            };
            self.seen[index] = true;
            *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                span: function.span,
                value: value.clone(),
            });
            return Ok(());
        }
        crate::plans::walk_expr_mut(self, expr)
    }

    fn visit_subquery_expr(&mut self, subquery: &'a mut SubqueryExpr) -> Result<()> {
        if let Some(child_expr) = subquery.child_expr.as_mut() {
            self.visit(child_expr)?;
        }
        subquery.subquery = Box::new(instantiate_s_expr(&subquery.subquery, self)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;

    use super::*;

    fn parse(sql: &str) -> Statement {
        let tokens = tokenize_sql(sql).unwrap();
        parse_sql(&tokens, Dialect::Experimental).unwrap().0
    }

    #[test]
    fn test_normalizes_predicate_literals_but_not_limit() {
        let left = ParameterizedStatement::create(&parse(
            "SELECT * FROM t WHERE a = 1 AND b = 'first' LIMIT 10",
        ))
        .unwrap();
        let right = ParameterizedStatement::create(&parse(
            "SELECT * FROM t WHERE a = 2 AND b = 'second' LIMIT 10",
        ))
        .unwrap();

        assert_eq!(left.cache_key_sql(), right.cache_key_sql());
        assert_eq!(left.values.len(), 2);
        assert_eq!(right.values.len(), 2);
        assert!(left.template.to_string().contains("LIMIT 10"));
    }

    #[test]
    fn test_parameter_type_and_limit_remain_in_cache_key() {
        let number =
            ParameterizedStatement::create(&parse("SELECT * FROM t WHERE a = 1 LIMIT 1")).unwrap();
        let string =
            ParameterizedStatement::create(&parse("SELECT * FROM t WHERE a = '1' LIMIT 1"))
                .unwrap();
        let other_limit =
            ParameterizedStatement::create(&parse("SELECT * FROM t WHERE a = 2 LIMIT 2")).unwrap();

        assert_ne!(number.cache_key_sql(), string.cache_key_sql());
        assert_ne!(number.cache_key_sql(), other_limit.cache_key_sql());
    }

    #[test]
    fn test_normalizes_in_and_between_literals() {
        let left = ParameterizedStatement::create(&parse(
            "SELECT * FROM t WHERE a IN (1, 2) AND b BETWEEN 'a' AND 'z'",
        ))
        .unwrap();
        let right = ParameterizedStatement::create(&parse(
            "SELECT * FROM t WHERE a IN (3, 4) AND b BETWEEN 'b' AND 'y'",
        ))
        .unwrap();

        assert_eq!(left.cache_key_sql(), right.cache_key_sql());
        assert_eq!(left.values.len(), 4);
    }

    #[test]
    fn test_normalizes_signed_like_and_distinct_literals() {
        let left = ParameterizedStatement::create(&parse(
            "SELECT * FROM t WHERE a = -1 AND b LIKE 'left%' AND c IS DISTINCT FROM 10",
        ))
        .unwrap();
        let right = ParameterizedStatement::create(&parse(
            "SELECT * FROM t WHERE a = -2 AND b LIKE 'right%' AND c IS DISTINCT FROM 20",
        ))
        .unwrap();

        assert_eq!(left.cache_key_sql(), right.cache_key_sql());
        assert_eq!(left.values.len(), 3);
    }

    #[test]
    fn test_existing_parameter_marker_disables_parameterization() {
        let statement = parse("SELECT __plan_parameter(0, 'user value') FROM t WHERE a = 1");
        let parameterized = ParameterizedStatement::create(&statement).unwrap();

        assert!(!parameterized.is_parameterized());
        assert!(parameterized.values.is_empty());
        assert_eq!(parameterized.template.to_string(), statement.to_string());
    }

    #[test]
    fn test_decimal_scale_remains_in_cache_key() {
        let left = ParameterizedStatement::create(&parse("SELECT * FROM t WHERE a = 1.1")).unwrap();
        let same_scale =
            ParameterizedStatement::create(&parse("SELECT * FROM t WHERE a = 2.2")).unwrap();
        let other_scale =
            ParameterizedStatement::create(&parse("SELECT * FROM t WHERE a = 2.22")).unwrap();

        assert_eq!(left.cache_key_sql(), same_scale.cache_key_sql());
        assert_ne!(left.cache_key_sql(), other_scale.cache_key_sql());
    }
}
