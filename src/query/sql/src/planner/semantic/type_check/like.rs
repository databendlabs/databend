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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::convert_escape_pattern;
use smallvec::smallvec;

use super::CoreExprArena;
use super::CoreExprId;
use super::scalar_rewrite::binary_op_core_function;
use super::scalar_rewrite::like_op_core_function;

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_special_binary_op_expr(
        &mut self,
        span: Span,
        op: &BinaryOperator,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<CoreExprId> {
        match op {
            BinaryOperator::NotLike(_) | BinaryOperator::NotRegexp | BinaryOperator::NotRLike => {
                let positive_op = match op {
                    BinaryOperator::NotLike(escape) => BinaryOperator::Like(escape.clone()),
                    BinaryOperator::NotRegexp => BinaryOperator::Regexp,
                    BinaryOperator::NotRLike => BinaryOperator::RLike,
                    _ => unreachable!(),
                };
                let positive =
                    self.lower_special_binary_op_expr(span, &positive_op, left, right)?;
                Ok(self.call(span, "not", smallvec![positive]))
            }
            BinaryOperator::SoundsLike => {
                let left = self.lower_ast_expr(left)?;
                let left = self.call(span, "soundex", smallvec![left]);
                let right = self.lower_ast_expr(right)?;
                let right = self.call(span, "soundex", smallvec![right]);
                Ok(self.call(span, "eq", smallvec![left, right]))
            }
            BinaryOperator::Like(escape) => {
                if let Expr::Literal {
                    value: Literal::String(like_str),
                    ..
                } = right
                {
                    return self.lower_like_expr(op, span, left, right, like_str, escape);
                }
                self.lower_like_escape_expr(span, "like", left, right, escape)
            }
            BinaryOperator::LikeAny(escape) => {
                self.lower_like_escape_expr(span, "like_any", left, right, escape)
            }
            BinaryOperator::Regexp => {
                self.lower_like_escape_expr(span, "regexp", left, right, &None)
            }
            BinaryOperator::RLike => self.lower_like_escape_expr(span, "rlike", left, right, &None),
            other => {
                let Some(func_name) = binary_op_core_function(other) else {
                    return Err(ErrorCode::Internal(format!(
                        "binary operator {other:?} should have been classified before scalar lowering",
                    )));
                };
                self.lower_call_expr(span, func_name, [left, right])
            }
        }
    }

    fn lower_like_expr(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &'a Expr,
        right: &'a Expr,
        like_str: &str,
        escape: &Option<String>,
    ) -> Result<CoreExprId> {
        let new_like_str = match escape.as_ref() {
            Some(escape_literal) => {
                let mut chars = escape_literal.chars();
                let Some(escape_char) = chars.next() else {
                    return self.lower_like_escape_expr(span, "like", left, right, escape);
                };

                if chars.next().is_some() {
                    return self.lower_like_escape_expr(span, "like", left, right, escape);
                }

                Cow::Owned(convert_escape_pattern(like_str, escape_char))
            }
            None => Cow::Borrowed(like_str),
        };

        if check_percent(&new_like_str) {
            let left = self.lower_ast_expr(left)?;
            Ok(self.call(span, "is_not_null", smallvec![left]))
        } else if check_const(&new_like_str) {
            self.lower_call_expr(span, "eq", [left, right])
        } else if check_prefix(&new_like_str) {
            let mut char_vec: Vec<char> = new_like_str[0..new_like_str.len() - 1].chars().collect();
            let len = char_vec.len();
            let ascii_val = *char_vec.last().unwrap() as u8 + 1;
            char_vec[len - 1] = ascii_val as char;
            let like_str_plus: String = char_vec.iter().collect();

            let left_gte = self.lower_ast_expr(left)?;
            let prefix = self.literal(
                None,
                Literal::String(new_like_str[..new_like_str.len() - 1].to_owned()),
            );
            let gte = self.call(span, "gte", smallvec![left_gte, prefix]);

            let left_lt = self.lower_ast_expr(left)?;
            let prefix_plus = self.literal(None, Literal::String(like_str_plus));
            let lt = self.call(span, "lt", smallvec![left_lt, prefix_plus]);

            Ok(self.call(span, "and", smallvec![gte, lt]))
        } else {
            let name =
                like_op_core_function(op).expect("LIKE operator should have a core function");
            self.lower_like_escape_expr(span, name, left, right, escape)
        }
    }

    pub(super) fn lower_like_escape_expr(
        &mut self,
        span: Span,
        func_name: &'static str,
        left: &'a Expr,
        right: &'a Expr,
        escape: &Option<String>,
    ) -> Result<CoreExprId> {
        let mut arguments = smallvec![self.lower_ast_expr(left)?, self.lower_ast_expr(right)?];
        if let Some(escape) = escape {
            arguments.push(self.literal(span, Literal::String(escape.clone())));
        }
        Ok(self.call(span, func_name, arguments))
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
