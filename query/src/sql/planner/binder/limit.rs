// Copyright 2022 Datafuse Labs.
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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::Binder;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::semantic::TypeChecker;
use crate::sql::plans::LimitPlan;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(super) async fn bind_limit(
        &mut self,
        child: SExpr,
        limit: Option<&Expr<'a>>,
        offset: &Option<Expr<'a>>,
        bind_context: &BindContext,
    ) -> Result<SExpr> {
        let type_checker = TypeChecker::new(bind_context, self.ctx.clone());

        let limit_cnt = match limit {
            Some(Expr::Literal { span: _, lit: x }) => {
                let data = type_checker.resolve_literal(x, None)?.as_u64()?;
                Some(data as usize)
            }
            Some(_) => {
                return Err(ErrorCode::IllegalDataType("Unsupported limit type"));
            }
            None => None,
        };

        let offset_cnt = if let Some(Expr::Literal { span: _, lit: x }) = offset {
            let data = type_checker.resolve_literal(x, None)?.as_u64()?;
            data as usize
        } else {
            0
        };

        let limit_plan = LimitPlan {
            limit: limit_cnt,
            offset: offset_cnt,
        };
        let new_expr = SExpr::create_unary(limit_plan.into(), child);
        Ok(new_expr)
    }
}
