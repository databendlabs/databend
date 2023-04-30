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

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::plans::Limit;

impl Binder {
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

    pub(super) fn bind_limit(child: SExpr, limit: Option<usize>, offset: usize) -> SExpr {
        let limit_plan = Limit { limit, offset };
        SExpr::create_unary(limit_plan.into(), child)
    }

    /// So far, we only support integer literal as limit argument.
    /// So we will try to extract the integer value from the AST directly.
    /// In the future it's possible to treat the argument as an expression.
    fn bind_limit_argument(expr: &Expr) -> Option<u64> {
        match expr {
            Expr::Literal {
                lit: Literal::UInt64(value),
                ..
            } => Some(*value),
            _ => None,
        }
    }
}
