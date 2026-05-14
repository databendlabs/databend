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

use std::borrow::Cow;

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_exception::Result;
use databend_common_expression::type_check::convert_escape_pattern;
use databend_common_expression::types::DataType;

use super::TypeChecker;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a> {
    pub(super) fn resolve_like(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &Expr,
        right: &Expr,
        like_str: &str,
        escape: &Option<String>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let new_like_str = match escape.as_ref() {
            Some(escape_literal) => {
                let mut chars = escape_literal.chars();
                let Some(escape_char) = chars.next() else {
                    // Empty escape literals must stay on the builtin path to match runtime behavior.
                    return self.resolve_like_escape(op, span, left, right, escape);
                };

                if chars.next().is_some() {
                    // Preserve existing builtin behavior for non-single-character escape literals.
                    return self.resolve_like_escape(op, span, left, right, escape);
                }

                Cow::Owned(convert_escape_pattern(like_str, escape_char))
            }
            None => Cow::Borrowed(like_str),
        };
        if check_percent(&new_like_str) {
            // Convert to `a is not null`
            let is_not_null = Expr::IsNull {
                span: None,
                expr: Box::new(left.clone()),
                not: true,
            };
            self.resolve(&is_not_null)
        } else if check_const(&new_like_str) {
            // Convert to equal comparison
            self.resolve_binary_op(span, &BinaryOperator::Eq, left, right)
        } else if check_prefix(&new_like_str) {
            // Convert to `a >= like_str and a < like_str + 1`
            let mut char_vec: Vec<char> = new_like_str[0..new_like_str.len() - 1].chars().collect();
            let len = char_vec.len();
            let ascii_val = *char_vec.last().unwrap() as u8 + 1;
            char_vec[len - 1] = ascii_val as char;
            let like_str_plus: String = char_vec.iter().collect();
            let (new_left, _) =
                *self.resolve_binary_op(span, &BinaryOperator::Gte, left, &Expr::Literal {
                    span: None,
                    value: Literal::String(new_like_str[..new_like_str.len() - 1].to_owned()),
                })?;
            let (new_right, _) =
                *self.resolve_binary_op(span, &BinaryOperator::Lt, left, &Expr::Literal {
                    span: None,
                    value: Literal::String(like_str_plus),
                })?;
            self.resolve_scalar_function_call(span, "and", vec![], vec![new_left, new_right])
        } else {
            self.resolve_like_escape(op, span, left, right, escape)
        }
    }

    pub(super) fn resolve_like_escape(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &Expr,
        right: &Expr,
        escape: &Option<String>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let name = op.to_func_name();
        let escape_expr = escape.as_ref().map(|escape| Expr::Literal {
            span,
            value: Literal::String(escape.clone()),
        });
        let mut arguments = vec![left, right];
        if let Some(expr) = &escape_expr {
            arguments.push(expr)
        }
        self.resolve_function(span, name.as_str(), vec![], &arguments)
    }
}

// optimize special cases for like expression
fn check_percent(like_str: &str) -> bool {
    !like_str.is_empty() && like_str.chars().all(|c| c == '%')
}

// Some check functions for like expression
fn check_const(like_str: &str) -> bool {
    for char in like_str.chars() {
        if char == '_' || char == '%' {
            return false;
        }
    }
    true
}

fn check_prefix(like_str: &str) -> bool {
    if like_str.contains("\\%") {
        return false;
    }
    if like_str.len() == 1 && matches!(like_str, "%" | "_") {
        return false;
    }
    if like_str.chars().filter(|c| *c == '%').count() != 1 {
        return false;
    }

    let mut i: usize = like_str.len();
    while i > 0 {
        if let Some(c) = like_str.chars().nth(i - 1) {
            if c != '%' {
                break;
            }
        } else {
            return false;
        }
        i -= 1;
    }
    if i == like_str.len() {
        return false;
    }
    for j in (0..i).rev() {
        if let Some(c) = like_str.chars().nth(j) {
            if c == '_' {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

// If `InList` expr satisfies the following conditions, it can be converted to `contain` function
// Note: the method mainly checks if list contains NULL literal, because `contain` can't handle NULL.
pub(super) fn satisfy_contain_func(expr: &Expr) -> bool {
    match expr {
        Expr::Literal { value, .. } => !matches!(value, Literal::Null),
        Expr::Tuple { exprs, .. } => {
            // For each expr in `exprs`, check if it satisfies the conditions
            exprs.iter().all(satisfy_contain_func)
        }
        Expr::Array { exprs, .. } => exprs.iter().all(satisfy_contain_func),
        // FIXME: others expr won't exist in `InList` expr
        _ => false,
    }
}
