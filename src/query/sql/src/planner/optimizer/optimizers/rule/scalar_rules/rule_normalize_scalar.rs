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

use databend_common_ast::Span;
use databend_common_exception::Result;
use databend_common_expression::Scalar;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::optimizers::rule::constant::is_falsy;
use crate::optimizer::optimizers::rule::constant::is_true;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

pub struct RuleNormalizeScalarFilter {
    matchers: Vec<Matcher>,
}

impl RuleNormalizeScalarFilter {
    pub fn new() -> Self {
        Self {
            matchers: vec![
                // Filter
                //  \
                //   *
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::Leaf],
                },
                // Scan
                Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                },
            ],
        }
    }
}

impl Rule for RuleNormalizeScalarFilter {
    fn id(&self) -> RuleID {
        RuleID::NormalizeScalarFilter
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let i = self
            .matchers
            .iter()
            .position(|matcher| matcher.matches(s_expr))
            .unwrap();
        self.apply_matcher(i, s_expr, state)
    }

    fn apply_matcher(&self, i: usize, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        match i {
            0 => {
                let filter = s_expr.plan.as_filter().unwrap();
                let Some(predicates) = RewritePredicates {}.rewrite(&filter.predicates)? else {
                    return Ok(());
                };
                state.add_result(s_expr.replace_plan(Filter { predicates }));
                Ok(())
            }
            1 => {
                let scan = s_expr.plan.as_scan().unwrap();
                let Some(predicates) = &scan.push_down_predicates else {
                    return Ok(());
                };
                let Some(predicates) = RewritePredicates {}.rewrite(predicates)? else {
                    return Ok(());
                };
                let mut scan = scan.clone();
                scan.push_down_predicates = Some(predicates);
                state.add_result(s_expr.replace_plan(scan));
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleNormalizeScalarFilter {
    fn default() -> Self {
        Self::new()
    }
}

struct RewritePredicates {}

impl RewritePredicates {
    fn rewrite(&mut self, predicates: &[ScalarExpr]) -> Result<Option<Vec<ScalarExpr>>> {
        let mut expr = if predicates.len() == 1 {
            predicates[0].clone()
        } else {
            FunctionCall {
                span: None,
                func_name: "and_filters".to_string(),
                params: vec![],
                arguments: predicates.to_vec(),
            }
            .into()
        };
        self.visit(&mut expr)?;

        match expr {
            ScalarExpr::FunctionCall(FunctionCall {
                func_name,
                arguments,
                ..
            }) if func_name == "and_filters" => {
                if arguments == predicates {
                    Ok(None)
                } else {
                    Ok(Some(arguments))
                }
            }
            expr => Ok(Some(vec![expr])),
        }
    }

    fn rewrite_and(
        &mut self,
        span: Span,
        arguments: &mut Vec<ScalarExpr>,
    ) -> Result<Option<ScalarExpr>> {
        let func_arguments = std::mem::take(arguments);
        for mut arg in func_arguments {
            self.visit(&mut arg)?;
            let inner_arguments = if let ScalarExpr::FunctionCall(call) = &mut arg
                && &call.func_name == "and_filters"
            {
                std::mem::take(&mut call.arguments)
            } else {
                vec![arg]
            };

            for arg in inner_arguments {
                if is_true(&arg) {
                    continue;
                }
                if is_falsy(&arg) {
                    return Ok(Some(
                        ConstantExpr {
                            span,
                            value: Scalar::Boolean(false),
                        }
                        .into(),
                    ));
                }
                arguments.push(arg)
            }
        }

        if arguments.is_empty() {
            return Ok(Some(
                ConstantExpr {
                    span,
                    value: Scalar::Boolean(true),
                }
                .into(),
            ));
        }
        if arguments.len() == 1 {
            return Ok(arguments.pop());
        }
        Ok(None)
    }

    fn rewrite_or(&mut self, arguments: &mut Vec<ScalarExpr>) -> Result<Option<ScalarExpr>> {
        let func_arguments = std::mem::take(arguments);
        for mut arg in func_arguments {
            self.visit(&mut arg)?;
            let inner_arguments = if let ScalarExpr::FunctionCall(call) = &mut arg
                && &call.func_name == "or_filters"
            {
                std::mem::take(&mut call.arguments)
            } else {
                vec![arg]
            };

            for arg in inner_arguments {
                if is_falsy(&arg) {
                    continue;
                }
                if is_true(&arg) {
                    return Ok(Some(arg));
                }
                arguments.push(arg);
            }
        }
        if arguments.len() == 1 {
            return Ok(arguments.pop());
        }
        Ok(None)
    }
}

impl<'a> VisitorMut<'a> for RewritePredicates {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        let new_expr = match expr {
            ScalarExpr::FunctionCall(FunctionCall {
                span,
                func_name,
                arguments,
                ..
            }) => match func_name.as_str() {
                "and" | "and_filters" => {
                    if func_name == "and" {
                        *func_name = "and_filters".to_string()
                    }
                    self.rewrite_and(*span, arguments)?
                }
                "or" | "or_filters" => {
                    if func_name == "or" {
                        *func_name = "or_filters".to_string()
                    }
                    self.rewrite_or(arguments)?
                }
                "not" => {
                    if let ScalarExpr::FunctionCall(inner) = &mut arguments[0]
                        && inner.func_name == "not"
                    {
                        Some(inner.arguments.pop().unwrap())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(new_expr) = new_expr {
            *expr = new_expr;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use databend_common_exception::Result;
    use databend_common_expression::RawExpr;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_functions::test_utils::parse_raw_expr;
    use goldenfile::Mint;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;
    use crate::plans::ScalarExpr;

    fn run_test(
        file: &mut impl Write,
        expr_text: &str,
        columns: &[(&str, DataType)],
    ) -> Result<()> {
        writeln!(file, "in           : {expr_text}")?;

        let raw_expr = parse_raw_expr(expr_text, columns);
        let mut expr = raw_expr_to_scalar(&raw_expr, columns);

        RewritePredicates {}.visit(&mut expr)?;

        writeln!(file, "out          : {}", expr.as_expr().unwrap())?;
        writeln!(file)?;
        Ok(())
    }

    fn raw_expr_to_scalar(raw_expr: &RawExpr, columns: &[(&str, DataType)]) -> ScalarExpr {
        match raw_expr {
            RawExpr::Constant { scalar, .. } => ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar.clone(),
            }),
            RawExpr::ColumnRef { id, .. } => {
                let index = *id;
                let (name, data_type) = &columns[index];
                let column = ColumnBindingBuilder::new(
                    name.to_string(),
                    index,
                    Box::new(data_type.clone()),
                    Visibility::Visible,
                )
                .build();
                ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
            }
            RawExpr::Cast {
                expr,
                dest_type,
                is_try,
                ..
            } => ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: *is_try,
                argument: Box::new(raw_expr_to_scalar(expr, columns)),
                target_type: Box::new(dest_type.clone()),
            }),
            RawExpr::FunctionCall {
                name, args, params, ..
            } => ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: name.clone(),
                params: params.clone(),
                arguments: args
                    .iter()
                    .map(|arg| raw_expr_to_scalar(arg, columns))
                    .collect(),
            }),
            RawExpr::LambdaFunctionCall { .. } => {
                unreachable!("lambda expressions are not used in tests")
            }
        }
    }

    #[test]
    fn test_rule_normalize_scalar() -> Result<()> {
        let mut mint = Mint::new("tests/ut/testdata");
        let file = &mut mint.new_goldenfile("rule_normalize_scalar.txt").unwrap();

        let columns = &[
            ("a", UInt64Type::data_type()),
            ("b", BooleanType::data_type()),
            ("c", UInt64Type::data_type().wrap_nullable()),
        ];

        run_test(file, "a = 5", columns)?;

        run_test(file, "a != 3 and a != 4 and a != 5", columns)?;
        run_test(file, "a != 3 and true and a != 5", columns)?;
        run_test(file, "a != 3 and false and a != 5", columns)?;

        run_test(file, "true and true", columns)?;

        run_test(file, "a = 3 or a = 4 or a = 5", columns)?;
        run_test(file, "a = 3 or true or a = 5", columns)?;
        run_test(file, "a = 3 or false or a = 5", columns)?;
        run_test(file, "a = 3 or false", columns)?;

        run_test(
            file,
            "(a = 9 or a = 8) and (a = 7 or a = 5) and a = 3",
            columns,
        )?;

        run_test(file, "not(not(b))", columns)?;

        run_test(file, "is_not_null(c < 3 and c < 4)", columns)?;

        Ok(())
    }
}
