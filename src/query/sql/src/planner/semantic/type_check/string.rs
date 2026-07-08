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
use databend_common_ast::ast::TrimWhere;
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
            BinaryOperator::NotLike(_)
            | BinaryOperator::NotILike(_)
            | BinaryOperator::NotRegexp
            | BinaryOperator::NotRLike => {
                let positive_op = match op {
                    BinaryOperator::NotLike(escape) => BinaryOperator::Like(escape.clone()),
                    BinaryOperator::NotILike(escape) => BinaryOperator::ILike(escape.clone()),
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
                    self.lower_like_expr(op, span, left, right, like_str, escape.as_ref())
                } else {
                    self.lower_like_escape_expr(span, "like", left, right, escape.as_ref())
                }
            }
            BinaryOperator::LikeAny(escape) => {
                self.lower_like_escape_expr(span, "like_any", left, right, escape.as_ref())
            }
            BinaryOperator::ILike(escape) => {
                self.lower_like_escape_expr(span, "ilike", left, right, escape.as_ref())
            }
            BinaryOperator::ILikeAny(escape) => {
                self.lower_like_escape_expr(span, "ilike_any", left, right, escape.as_ref())
            }
            BinaryOperator::Regexp => {
                self.lower_like_escape_expr(span, "regexp", left, right, None)
            }
            BinaryOperator::RLike => self.lower_like_escape_expr(span, "rlike", left, right, None),
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

    pub(super) fn lower_substring_expr(
        &mut self,
        span: Span,
        expr: &'a Expr,
        substring_from: &'a Expr,
        substring_for: Option<&'a Expr>,
    ) -> Result<CoreExprId> {
        let expr = self.lower_ast_expr(expr)?;
        let substring_from = self.lower_ast_expr(substring_from)?;
        let mut arguments = smallvec![expr, substring_from];
        if let Some(substring_for) = substring_for {
            arguments.push(self.lower_ast_expr(substring_for)?);
        }
        Ok(self.call(span, "substring", arguments))
    }

    pub(super) fn lower_trim_expr(
        &mut self,
        span: Span,
        expr: &'a Expr,
        trim_where: &'a Option<(TrimWhere, Box<Expr>)>,
    ) -> Result<CoreExprId> {
        let (func_name, trim_arg) = if let Some((trim_type, trim_expr)) = trim_where {
            let func_name = match trim_type {
                TrimWhere::Leading => "trim_leading",
                TrimWhere::Trailing => "trim_trailing",
                TrimWhere::Both => "trim_both",
            };
            (func_name, self.lower_ast_expr(trim_expr.as_ref())?)
        } else {
            let trim_arg = self.literal(span, Literal::String(" ".to_string()));
            ("trim_both", trim_arg)
        };
        let expr = self.lower_ast_expr(expr)?;
        Ok(self.call(span, func_name, smallvec![expr, trim_arg]))
    }

    fn lower_like_expr(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &'a Expr,
        right: &'a Expr,
        like_str: &str,
        escape: Option<&String>,
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
        } else if let Some((prefix, prefix_plus)) = like_prefix_range(&new_like_str) {
            let left_gte = self.lower_ast_expr(left)?;
            let prefix = self.literal(None, Literal::String(prefix.to_owned()));
            let gte = self.call(span, "gte", smallvec![left_gte, prefix]);

            let left_lt = self.lower_ast_expr(left)?;
            let prefix_plus = self.literal(None, Literal::String(prefix_plus));
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
        escape: Option<&String>,
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

fn like_prefix_range(like_str: &str) -> Option<(&str, String)> {
    if like_str.contains("\\%") {
        return None;
    }

    let prefix = like_str.strip_suffix('%')?;
    if prefix.is_empty() || prefix.contains('%') || prefix.contains('_') {
        return None;
    };

    Some((prefix, next_utf8_prefix(prefix)?))
}

fn next_utf8_prefix(prefix: &str) -> Option<String> {
    if let Some(last) = prefix.as_bytes().last()
        && last.is_ascii()
        && *last < 0x7f
    {
        let mut upper_bound = prefix.as_bytes().to_vec();
        *upper_bound.last_mut()? += 1;
        return String::from_utf8(upper_bound).ok();
    }

    for (index, ch) in prefix.char_indices().rev() {
        if let Some(next) = next_unicode_scalar(ch) {
            let mut upper_bound = String::with_capacity(index + next.len_utf8());
            upper_bound.push_str(&prefix[..index]);
            upper_bound.push(next);
            return Some(upper_bound);
        }
    }
    None
}

fn next_unicode_scalar(ch: char) -> Option<char> {
    let mut code = ch as u32;
    while code < char::MAX as u32 {
        code += 1;
        if let Some(next) = char::from_u32(code) {
            return Some(next);
        }
    }
    None
}
