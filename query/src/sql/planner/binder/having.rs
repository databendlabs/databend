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
use common_ast::parser::token::Token;
use common_exception::Result;

use crate::sql::binder::aggregate::AggregateRewriter;
use crate::sql::binder::split_conjunctions;
use crate::sql::binder::ScalarBinder;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::semantic::GroupingChecker;
use crate::sql::plans::Filter;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::Binder;

impl<'a> Binder {
    /// Analyze aggregates in having clause, this will rewrite aggregate functions.
    /// See `AggregateRewriter` for more details.
    pub(super) async fn analyze_aggregate_having(
        &mut self,
        bind_context: &mut BindContext,
        having: &Expr<'a>,
    ) -> Result<(Scalar, &'a [Token<'a>])> {
        let mut scalar_binder =
            ScalarBinder::new(bind_context, self.ctx.clone(), self.metadata.clone());
        let (scalar, _) = scalar_binder.bind(having).await?;
        let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
        Ok((rewriter.visit(&scalar)?, having.span()))
    }

    pub(super) async fn bind_having(
        &mut self,
        bind_context: &BindContext,
        having: Scalar,
        span: &'a [Token<'a>],
        child: SExpr,
    ) -> Result<SExpr> {
        let scalar = if bind_context.in_grouping {
            // If we are in grouping context, we will perform the grouping check
            let mut grouping_checker = GroupingChecker::new(bind_context);
            grouping_checker.resolve(&having, Some(span))?
        } else {
            // Otherwise we just fallback to a normal selection as `WHERE` clause.
            // This follows behavior of MySQL and Snowflake.
            having
        };

        let predicates = split_conjunctions(&scalar);

        let filter = Filter {
            predicates,
            is_having: true,
        };

        Ok(SExpr::create_unary(filter.into(), child))
    }
}
