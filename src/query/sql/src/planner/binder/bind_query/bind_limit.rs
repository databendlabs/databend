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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::plans::Limit;

impl Binder {
    pub(super) fn bind_query_limit(
        &self,
        query: &Query,
        s_expr: SExpr,
        limit: Option<usize>,
        offset: usize,
    ) -> SExpr {
        if limit.is_none() && query.offset.is_none() {
            return s_expr;
        }

        let limit_plan = Limit {
            before_exchange: false,
            limit,
            offset,
        };
        SExpr::create_unary(Arc::new(limit_plan.into()), Arc::new(s_expr))
    }

    pub(crate) fn extract_limit_and_offset(&self, query: &Query) -> Result<(Option<usize>, usize)> {
        let (mut limit, offset) = if !query.limit.is_empty() {
            if query.limit.len() == 1 {
                Self::analyze_limit(Some(&query.limit[0]), &query.offset)?
            } else {
                Self::analyze_limit(Some(&query.limit[1]), &Some(query.limit[0].clone()))?
            }
        } else if query.offset.is_some() {
            Self::analyze_limit(None, &query.offset)?
        } else {
            (None, 0)
        };

        if let SetExpr::Select(stmt) = &query.body {
            if !query.limit.is_empty() && stmt.top_n.is_some() {
                return Err(ErrorCode::SemanticError(
                    "Duplicate LIMIT: TopN and Limit cannot be used together",
                ));
            } else if let Some(n) = stmt.top_n {
                limit = Some(n as usize);
            }
        }

        Ok((limit, offset))
    }

    pub(super) fn analyze_limit(
        limit: Option<&Expr>,
        offset: &Option<Expr>,
    ) -> Result<(Option<usize>, usize)> {
        let limit_cnt = match limit {
            Some(limit) => Some(
                Self::bind_limit_argument(limit)
                    .ok_or_else(|| ErrorCode::SemanticError("Invalid LIMIT expression"))?
                    as usize,
            ),
            None => None,
        };

        let offset_cnt = if let Some(offset) = offset {
            Self::bind_limit_argument(offset)
                .ok_or_else(|| ErrorCode::SemanticError("Invalid OFFSET expression"))?
                as usize
        } else {
            0
        };

        Ok((limit_cnt, offset_cnt))
    }

    /// So far, we only support integer literal as limit argument.
    /// So we will try to extract the integer value from the AST directly.
    /// In the future it's possible to treat the argument as an expression.
    fn bind_limit_argument(expr: &Expr) -> Option<u64> {
        match expr {
            Expr::Literal {
                value: Literal::UInt64(value),
                ..
            } => Some(*value),
            _ => None,
        }
    }
}
