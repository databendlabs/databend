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
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::TrimWhere;
use databend_common_exception::Result;
use smallvec::smallvec;

use super::CoreExprArena;
use super::CoreExprId;

impl<'a> CoreExprArena<'a> {
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
}
